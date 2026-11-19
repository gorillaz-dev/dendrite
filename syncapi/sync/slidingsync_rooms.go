// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"

	roomserverAPI "github.com/element-hq/dendrite/roomserver/api"
	rstypes "github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/synctypes"
	"github.com/element-hq/dendrite/syncapi/types"
	userapi "github.com/element-hq/dendrite/userapi/api"
)

// slidingSyncBumpEventTypes are the event types which count as "proper" activity
// for the purpose of calculating the bump_stamp of a room.
var slidingSyncBumpEventTypes = []string{
	spec.MRoomCreate,
	"m.room.message",
	"m.room.encrypted",
	"m.sticker",
	"m.call.invite",
	"m.poll.start",
	"m.beacon_info",
}

// slidingSyncRoomEntry is a room in the server-side room list of a user.
type slidingSyncRoomEntry struct {
	roomID string
	// The current membership of the user: join, invite, knock, leave or ban.
	membership string
	// The position used to order the room list. Greater is more recent.
	sortPos types.StreamPosition
	// The PDU stream position of the user's membership event, if known.
	membershipPos types.StreamPosition
	// The user's current membership event in the room, if known.
	membershipEventID string
	// True if the user has been joined to the room at some point.
	everJoined bool
	// For invites: the invite event. For rejected invites: the retired invite event, if any.
	inviteEvent *rstypes.HeaderedEvent
	// True if the room is part of the server-side room list. Rooms which are not part
	// of the list can only be returned because of a room subscription.
	inList bool
}

// full returns true if the full room data may be returned to the user, which is the
// case for rooms the user is or has been joined to. For all other rooms only stripped
// state is returned.
func (e *slidingSyncRoomEntry) full() bool {
	switch e.membership {
	case spec.Join:
		return true
	case spec.Leave, spec.Ban:
		return e.everJoined
	default:
		return false
	}
}

// slidingSyncRoomList is the server-side room list of a user.
type slidingSyncRoomList struct {
	// The rooms in the list, ordered by activity (most recent first).
	sorted []*slidingSyncRoomEntry
	// All rooms the user may subscribe to, including those in sorted.
	byID map[string]*slidingSyncRoomEntry
}

// buildRoomList builds the server-side room list for the user: rooms the user is joined to,
// invited to or knocked on, rooms the user has been kicked or banned from after being joined,
// and rooms the user has left if they were previously sent on this connection.
// nolint:gocyclo
func (rp *RequestPool) buildRoomList(
	ctx context.Context, snapshot storage.DatabaseTransaction, conn *slidingSyncConnection,
	device *userapi.Device, base *slidingSyncSnapshot, currentPos types.StreamPosition, ignored *types.IgnoredUsers,
) (*slidingSyncRoomList, error) {
	memberships, err := snapshot.MembershipEventsForUser(ctx, device.UserID)
	if err != nil {
		return nil, fmt.Errorf("snapshot.MembershipEventsForUser: %w", err)
	}
	maxInvitePos, err := snapshot.MaxStreamPositionForInvites(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot.MaxStreamPositionForInvites: %w", err)
	}
	invites, retiredInvites, _, err := snapshot.InviteEventsInRange(ctx, device.UserID, types.Range{From: 0, To: maxInvitePos})
	if err != nil {
		return nil, fmt.Errorf("snapshot.InviteEventsInRange: %w", err)
	}

	list := &slidingSyncRoomList{byID: make(map[string]*slidingSyncRoomEntry)}

	// Work out which of the rooms the user is no longer joined to they have been joined
	// to at some point. This only changes with the membership event, so it is cached.
	var joinedRoomIDs, checkEverJoined []string
	everJoined := make(map[string]struct{})
	for roomID, m := range memberships {
		switch m.Membership {
		case spec.Join:
			joinedRoomIDs = append(joinedRoomIDs, roomID)
		case spec.Leave, spec.Ban:
			if cached, ok := conn.getEverJoined(roomID, m.EventID); !ok {
				checkEverJoined = append(checkEverJoined, roomID)
			} else if cached {
				everJoined[roomID] = struct{}{}
			}
		}
	}
	checked, err := snapshot.RoomsEverJoined(ctx, device.UserID, checkEverJoined)
	if err != nil {
		return nil, fmt.Errorf("snapshot.RoomsEverJoined: %w", err)
	}
	for _, roomID := range checkEverJoined {
		_, ok := checked[roomID]
		if ok {
			everJoined[roomID] = struct{}{}
		}
		conn.setEverJoined(roomID, memberships[roomID].EventID, ok)
	}
	latestPositions, err := snapshot.MaxStreamPositionsForRooms(ctx, joinedRoomIDs, nil, currentPos)
	if err != nil {
		return nil, fmt.Errorf("snapshot.MaxStreamPositionsForRooms: %w", err)
	}

	for roomID, m := range memberships {
		entry := &slidingSyncRoomEntry{
			roomID:            roomID,
			membership:        m.Membership,
			sortPos:           m.StreamPos,
			membershipPos:     m.StreamPos,
			membershipEventID: m.EventID,
		}
		_, previouslySent := base.rooms[roomID]
		switch m.Membership {
		case spec.Join:
			entry.everJoined = true
			entry.inList = true
			if pos, ok := latestPositions[roomID]; ok && pos > entry.sortPos {
				entry.sortPos = pos
			}
		case spec.Invite:
			// Invites are handled using the invites table below, which also contains
			// invites to rooms the server is not participating in.
			continue
		case spec.Knock:
			entry.inList = true
		case spec.Leave, spec.Ban:
			_, entry.everJoined = everJoined[roomID]
			kicked := m.Membership == spec.Leave && m.Sender != device.UserID
			switch {
			case previouslySent:
				// The client needs to learn about the membership change.
				entry.inList = true
			case entry.everJoined && (kicked || m.Membership == spec.Ban):
				// Rooms the user has been kicked or banned from are always included,
				// as the user may not have seen the change otherwise.
				entry.inList = true
			}
			if retired, ok := retiredInvites[roomID]; ok && !entry.everJoined {
				entry.inviteEvent = retired
			}
		default:
			continue
		}
		list.byID[roomID] = entry
	}
	for roomID, inviteEvent := range invites {
		if m, ok := memberships[roomID]; ok && m.Membership == spec.Join {
			continue
		}
		sender, err := rp.rsAPI.QueryUserIDForSender(ctx, inviteEvent.RoomID(), inviteEvent.SenderID())
		if err == nil && sender != nil {
			if _, ok := ignored.List[sender.String()]; ok {
				continue
			}
		}
		entry := &slidingSyncRoomEntry{
			roomID:      roomID,
			membership:  spec.Invite,
			inviteEvent: inviteEvent,
			inList:      true,
			// Invites to rooms the server is not participating in don't have a position in
			// the PDU stream, so they are ordered as if they just happened.
			sortPos: currentPos,
		}
		if m, ok := memberships[roomID]; ok && m.Membership == spec.Invite && m.StreamPos > 0 {
			entry.sortPos = m.StreamPos
			entry.membershipPos = m.StreamPos
			entry.membershipEventID = m.EventID
		}
		list.byID[roomID] = entry
	}

	// Invites which were rejected (or revoked) without the server participating in the room
	// don't result in a membership event. Tell the client about them if it saw the invite.
	for roomID, retired := range retiredInvites {
		if _, ok := list.byID[roomID]; ok {
			continue
		}
		if prev, ok := base.rooms[roomID]; !ok || prev.Membership != spec.Invite {
			continue
		}
		list.byID[roomID] = &slidingSyncRoomEntry{
			roomID:      roomID,
			membership:  spec.Leave,
			sortPos:     currentPos,
			inviteEvent: retired,
			inList:      true,
		}
	}

	// Forgotten rooms are not returned. Only rooms the user is not joined to can be forgotten.
	// Rooms which are not part of the list are checked when they are subscribed to.
	for roomID, entry := range list.byID {
		if entry.membership != spec.Leave && entry.membership != spec.Ban {
			continue
		}
		if !entry.inList {
			if !entry.everJoined {
				// Neither part of the list nor available to room subscriptions.
				delete(list.byID, roomID)
			}
			continue
		}
		forgotten, err := rp.isRoomForgotten(ctx, device.UserID, roomID)
		if err != nil {
			return nil, err
		}
		if forgotten {
			delete(list.byID, roomID)
		}
	}

	for _, entry := range list.byID {
		if entry.inList {
			list.sorted = append(list.sorted, entry)
		}
	}
	sort.Slice(list.sorted, func(i, j int) bool {
		a, b := list.sorted[i], list.sorted[j]
		if a.sortPos != b.sortPos {
			return a.sortPos > b.sortPos
		}
		return a.roomID < b.roomID
	})
	return list, nil
}

// isRoomForgotten returns true if the user has forgotten the room.
func (rp *RequestPool) isRoomForgotten(ctx context.Context, userID, roomID string) (bool, error) {
	parsedUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return false, err
	}
	res := roomserverAPI.QueryMembershipForUserResponse{}
	if err = rp.rsAPI.QueryMembershipForUser(ctx, &roomserverAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *parsedUserID,
	}, &res); err != nil {
		return false, fmt.Errorf("rsAPI.QueryMembershipForUser: %w", err)
	}
	return res.IsRoomForgotten, nil
}

// slidingSyncFilterContext lazily loads the data needed to evaluate list filters
// and caches it for the duration of a request.
type slidingSyncFilterContext struct {
	ctx      context.Context
	rp       *RequestPool
	snapshot storage.DatabaseTransaction
	conn     *slidingSyncConnection
	device   *userapi.Device
	list     *slidingSyncRoomList

	dmRooms       map[string]struct{}
	tags          map[string]map[string]struct{}
	spaceChildren map[string]map[string]struct{}
}

// isDM returns true if the room is a direct message room, according to the m.direct
// account data, or if the user was invited to the room with is_direct set.
func (f *slidingSyncFilterContext) isDM(entry *slidingSyncRoomEntry) (bool, error) {
	if f.dmRooms == nil {
		f.dmRooms = make(map[string]struct{})
		res := userapi.QueryAccountDataResponse{}
		if err := f.rp.userAPI.QueryAccountData(f.ctx, &userapi.QueryAccountDataRequest{
			UserID:   f.device.UserID,
			DataType: "m.direct",
		}, &res); err != nil {
			return false, fmt.Errorf("userAPI.QueryAccountData: %w", err)
		}
		var direct map[string][]string
		if data, ok := res.GlobalAccountData["m.direct"]; ok {
			// Ignore malformed account data, as it is controlled by clients.
			_ = json.Unmarshal(data, &direct)
		}
		for _, roomIDs := range direct {
			for _, roomID := range roomIDs {
				f.dmRooms[roomID] = struct{}{}
			}
		}
	}
	if _, ok := f.dmRooms[entry.roomID]; ok {
		return true, nil
	}
	if entry.membership == spec.Invite && entry.inviteEvent != nil {
		return gjson.GetBytes(entry.inviteEvent.Content(), "is_direct").Bool(), nil
	}
	return false, nil
}

// roomTags returns the tags of the room, from the m.tag room account data.
func (f *slidingSyncFilterContext) roomTags(roomID string) (map[string]struct{}, error) {
	if f.tags == nil {
		f.tags = make(map[string]map[string]struct{})
		res := userapi.QueryAccountDataResponse{}
		if err := f.rp.userAPI.QueryAccountData(f.ctx, &userapi.QueryAccountDataRequest{
			UserID: f.device.UserID,
		}, &res); err != nil {
			return nil, fmt.Errorf("userAPI.QueryAccountData: %w", err)
		}
		for rID, data := range res.RoomAccountData {
			tagData, ok := data["m.tag"]
			if !ok {
				continue
			}
			var content struct {
				Tags map[string]json.RawMessage `json:"tags"`
			}
			// Ignore malformed account data, as it is controlled by clients.
			if err := json.Unmarshal(tagData, &content); err != nil {
				continue
			}
			roomTags := make(map[string]struct{}, len(content.Tags))
			for tag := range content.Tags {
				roomTags[tag] = struct{}{}
			}
			f.tags[rID] = roomTags
		}
	}
	return f.tags[roomID], nil
}

// isSpaceChild returns true if the room is a direct child of one of the given spaces.
// Only spaces the user is joined to are considered.
func (f *slidingSyncFilterContext) isSpaceChild(roomID string, spaces []string) (bool, error) {
	if f.spaceChildren == nil {
		f.spaceChildren = make(map[string]map[string]struct{})
	}
	for _, spaceID := range spaces {
		children, ok := f.spaceChildren[spaceID]
		if !ok {
			children = make(map[string]struct{})
			if entry, ok := f.list.byID[spaceID]; ok && entry.membership == spec.Join {
				filter := synctypes.DefaultStateFilter()
				filter.Types = &[]string{"m.space.child"}
				events, err := f.snapshot.CurrentState(f.ctx, spaceID, &filter, nil)
				if err != nil {
					return false, fmt.Errorf("snapshot.CurrentState: %w", err)
				}
				for _, ev := range events {
					// Children without a valid "via" are not considered part of the space.
					if ev.Type() != "m.space.child" || ev.StateKey() == nil {
						continue
					}
					if via := gjson.GetBytes(ev.Content(), "via"); !via.IsArray() || len(via.Array()) == 0 {
						continue
					}
					children[*ev.StateKey()] = struct{}{}
				}
			}
			f.spaceChildren[spaceID] = children
		}
		if _, ok := children[roomID]; ok {
			return true, nil
		}
	}
	return false, nil
}

// facts returns the room type and encryption state of the room, using only information
// the user is allowed to see.
func (f *slidingSyncFilterContext) facts(entry *slidingSyncRoomEntry) (roomFacts, error) {
	if !entry.full() {
		// Only the stripped state is available to the user.
		facts := roomFacts{hasRoomType: true}
		for _, raw := range strippedStateForEntry(entry) {
			switch gjson.GetBytes(raw, "type").Str {
			case spec.MRoomCreate:
				if roomType := gjson.GetBytes(raw, "content.type"); roomType.Type == gjson.String {
					facts.roomType = &roomType.Str
				}
			case spec.MRoomEncryption:
				facts.isEncrypted = true
			}
		}
		return facts, nil
	}

	facts := f.conn.getFacts(entry.roomID)
	if !facts.hasRoomType {
		createEvent, err := f.snapshot.GetStateEvent(f.ctx, entry.roomID, spec.MRoomCreate, "")
		if err != nil {
			return facts, fmt.Errorf("snapshot.GetStateEvent: %w", err)
		}
		if createEvent != nil {
			if roomType := gjson.GetBytes(createEvent.Content(), "type"); roomType.Type == gjson.String {
				facts.roomType = &roomType.Str
			}
			facts.hasRoomType = true
			f.conn.setFacts(entry.roomID, facts)
		}
	}
	if !facts.isEncrypted {
		encryptionEvent, err := f.snapshot.GetStateEvent(f.ctx, entry.roomID, spec.MRoomEncryption, "")
		if err != nil {
			return facts, fmt.Errorf("snapshot.GetStateEvent: %w", err)
		}
		if encryptionEvent != nil {
			if entry.membership == spec.Join {
				facts.isEncrypted = true
				f.conn.setFacts(entry.roomID, facts)
			} else {
				// Don't reveal whether the room was encrypted after the user left.
				_, pos, err := f.snapshot.PositionInTopology(f.ctx, encryptionEvent.EventID())
				if err == nil && pos <= entry.membershipPos {
					return roomFacts{hasRoomType: facts.hasRoomType, roomType: facts.roomType, isEncrypted: true}, nil
				}
			}
		}
	}
	return facts, nil
}

// matches returns true if the room matches the list filters.
// nolint:gocyclo
func (f *slidingSyncFilterContext) matches(entry *slidingSyncRoomEntry, filters *types.SlidingSyncRoomFilter) (bool, error) {
	if filters == nil {
		return true, nil
	}
	if filters.IsInvited != nil && (entry.membership == spec.Invite) != *filters.IsInvited {
		return false, nil
	}
	if filters.IsDM != nil {
		isDM, err := f.isDM(entry)
		if err != nil {
			return false, err
		}
		if isDM != *filters.IsDM {
			return false, nil
		}
	}
	if len(filters.Spaces) > 0 {
		isChild, err := f.isSpaceChild(entry.roomID, filters.Spaces)
		if err != nil {
			return false, err
		}
		if !isChild {
			return false, nil
		}
	}
	if filters.IsEncrypted != nil || filters.RoomTypes != nil || filters.NotRoomTypes != nil {
		facts, err := f.facts(entry)
		if err != nil {
			return false, err
		}
		if filters.IsEncrypted != nil && facts.isEncrypted != *filters.IsEncrypted {
			return false, nil
		}
		if containsRoomType(filters.NotRoomTypes, facts.roomType) {
			return false, nil
		}
		if filters.RoomTypes != nil && !containsRoomType(filters.RoomTypes, facts.roomType) {
			return false, nil
		}
	}
	if len(filters.Tags) > 0 || len(filters.NotTags) > 0 {
		tags, err := f.roomTags(entry.roomID)
		if err != nil {
			return false, err
		}
		for _, tag := range filters.NotTags {
			if _, ok := tags[tag]; ok {
				return false, nil
			}
		}
		if len(filters.Tags) > 0 && !slices.ContainsFunc(filters.Tags, func(tag string) bool {
			_, ok := tags[tag]
			return ok
		}) {
			return false, nil
		}
	}
	return true, nil
}

// containsRoomType returns true if roomType is in roomTypes. A nil entry in roomTypes
// matches rooms without a room type.
func containsRoomType(roomTypes []*string, roomType *string) bool {
	for _, t := range roomTypes {
		if t == nil && roomType == nil {
			return true
		}
		if t != nil && roomType != nil && *t == *roomType {
			return true
		}
	}
	return false
}

// slidingSyncRoomConfig is the combination of all room configs that apply to a room.
type slidingSyncRoomConfig struct {
	timelineLimit int
	requiredState []types.SlidingSyncRequiredState
	lists         []string
}

// add combines the given room config into this one, by taking the superset of both.
func (c *slidingSyncRoomConfig) add(timelineLimit int, requiredState types.SlidingSyncRequiredState) {
	c.timelineLimit = max(c.timelineLimit, min(timelineLimit, types.SlidingSyncMaxTimelineLimit))
	for _, existing := range c.requiredState {
		if requiredStateEqual(existing, requiredState) {
			return
		}
	}
	c.requiredState = append(c.requiredState, requiredState)
}

// includesState returns true if the combined config returns the given state as part of
// the required state, without taking lazy loading into account.
func includesState(configs []types.SlidingSyncRequiredState, eventType, stateKey string) bool {
	for _, rs := range configs {
		if requiredStateIncludes(rs, eventType, stateKey) {
			return true
		}
	}
	return false
}

// requiredStateIncludes returns true if the state matches the include filters and none of
// the exclude filters. If lazy loading of members is enabled, membership events only match
// filters which explicitly name the state key.
func requiredStateIncludes(rs types.SlidingSyncRequiredState, eventType, stateKey string) bool {
	included := slices.ContainsFunc(rs.Include, func(e types.SlidingSyncRequiredStateElement) bool {
		if !e.Matches(eventType, stateKey) {
			return false
		}
		return !rs.LazyMembers || eventType != spec.MRoomMember || e.StateKey != nil
	})
	if !included {
		return false
	}
	return !slices.ContainsFunc(rs.Exclude, func(e types.SlidingSyncRequiredStateElement) bool {
		return e.Matches(eventType, stateKey)
	})
}

// lazyMembers returns true if lazy loading of members applies to the combined config: that
// is the case if any config enables it, and no config returns all members without lazy loading.
func lazyMembers(configs []types.SlidingSyncRequiredState) bool {
	lazy := false
	for _, rs := range configs {
		if rs.LazyMembers {
			lazy = true
			continue
		}
		returnsAllMembers := slices.ContainsFunc(rs.Include, func(e types.SlidingSyncRequiredStateElement) bool {
			return (e.Type == nil || *e.Type == spec.MRoomMember) && e.StateKey == nil
		}) && !slices.ContainsFunc(rs.Exclude, func(e types.SlidingSyncRequiredStateElement) bool {
			return e.Type != nil && *e.Type == spec.MRoomMember
		})
		if returnsAllMembers {
			return false
		}
	}
	return lazy
}

func requiredStateEqual(a, b types.SlidingSyncRequiredState) bool {
	return a.LazyMembers == b.LazyMembers &&
		slices.EqualFunc(a.Include, b.Include, requiredStateElementEqual) &&
		slices.EqualFunc(a.Exclude, b.Exclude, requiredStateElementEqual)
}

// requiredStateConfigsEqual returns true if both lists contain the same configs, in any order.
func requiredStateConfigsEqual(a, b []types.SlidingSyncRequiredState) bool {
	contains := func(configs []types.SlidingSyncRequiredState, rs types.SlidingSyncRequiredState) bool {
		return slices.ContainsFunc(configs, func(other types.SlidingSyncRequiredState) bool {
			return requiredStateEqual(rs, other)
		})
	}
	for _, rs := range a {
		if !contains(b, rs) {
			return false
		}
	}
	for _, rs := range b {
		if !contains(a, rs) {
			return false
		}
	}
	return true
}

func requiredStateElementEqual(a, b types.SlidingSyncRequiredStateElement) bool {
	return optionalStringEqual(a.Type, b.Type) && optionalStringEqual(a.StateKey, b.StateKey)
}

func optionalStringEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// strippedStateForEntry returns the stripped state of a room the user has not been joined to.
func strippedStateForEntry(entry *slidingSyncRoomEntry) []json.RawMessage {
	if entry.inviteEvent == nil {
		return nil
	}
	if roomState := gjson.GetBytes(entry.inviteEvent.Unsigned(), "invite_room_state"); roomState.IsArray() {
		var events []json.RawMessage
		if err := json.Unmarshal([]byte(roomState.Raw), &events); err == nil {
			return events
		}
	}
	return nil
}
