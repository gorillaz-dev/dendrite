// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"

	rstypes "github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/streams"
	"github.com/element-hq/dendrite/syncapi/synctypes"
	"github.com/element-hq/dendrite/syncapi/types"
	userapi "github.com/element-hq/dendrite/userapi/api"
)

// slidingSyncRequest holds the state needed to calculate the room results of a request.
type slidingSyncRequest struct {
	ctx      context.Context
	rp       *RequestPool
	snapshot storage.DatabaseTransaction
	conn     *slidingSyncConnection
	device   *userapi.Device
	base     *slidingSyncSnapshot
	filters  *slidingSyncFilterContext
	// The PDU position the response is calculated at.
	currentPos types.StreamPosition
	// Senders the user is ignoring.
	ignored []string
	// The bump stamps of the joined rooms being processed.
	bumpStamps map[string]types.StreamPosition
}

func (r *slidingSyncRequest) userIDForSender(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return r.rp.rsAPI.QueryUserIDForSender(r.ctx, roomID, senderID)
}

// roomResult calculates the result for a single room. It returns a nil result if there is
// nothing new to send for the room. The returned state is what the connection should remember
// about the room afterwards.
func (r *slidingSyncRequest) roomResult(
	entry *slidingSyncRoomEntry, cfg *slidingSyncRoomConfig, prev *slidingSyncRoomState,
) (*types.SlidingSyncRoom, *slidingSyncRoomState, error) {
	isDM, err := r.filters.isDM(entry)
	if err != nil {
		return nil, nil, err
	}
	if !entry.full() {
		return r.strippedRoomResult(entry, cfg, prev, isDM)
	}
	initial := prev == nil || prev.Stripped || (entry.membership == spec.Join && prev.Membership != spec.Join)
	if initial {
		prev = nil
	}
	return r.fullRoomResult(entry, cfg, prev, isDM)
}

// strippedRoomResult returns the result for a room the user has not been joined to.
func (r *slidingSyncRequest) strippedRoomResult(
	entry *slidingSyncRoomEntry, cfg *slidingSyncRoomConfig, prev *slidingSyncRoomState, isDM bool,
) (*types.SlidingSyncRoom, *slidingSyncRoomState, error) {
	eventID := entry.membershipEventID
	if entry.inviteEvent != nil {
		eventID = entry.inviteEvent.EventID() + "|" + eventID
	}
	if prev != nil && prev.Stripped && prev.Membership == entry.membership && prev.StrippedEventID == eventID {
		return nil, prev, nil
	}

	strippedState, err := r.strippedState(entry)
	if err != nil {
		return nil, nil, err
	}
	bumpStamp := int64(entry.sortPos)
	if entry.membershipPos == 0 && prev != nil && prev.Stripped && prev.StrippedEventID == eventID {
		bumpStamp = prev.BumpStamp
	}
	state := &slidingSyncRoomState{
		Membership:      entry.membership,
		Stripped:        true,
		StrippedEventID: eventID,
		BumpStamp:       bumpStamp,
		IsDM:            isDM,
	}
	res := &types.SlidingSyncRoom{
		BumpStamp:     &bumpStamp,
		Membership:    entry.membership,
		Lists:         cfg.lists,
		StrippedState: strippedState,
	}
	return res, state, nil
}

// strippedState returns the stripped state for a room the user has not been joined to.
func (r *slidingSyncRequest) strippedState(entry *slidingSyncRoomEntry) ([]json.RawMessage, error) {
	strippedState := []json.RawMessage{}
	if entry.inviteEvent != nil {
		inviteRes, err := types.NewInviteResponse(r.ctx, r.rp.rsAPI, entry.inviteEvent, synctypes.FormatSync)
		if err != nil {
			return nil, fmt.Errorf("types.NewInviteResponse: %w", err)
		}
		strippedState = append(strippedState, inviteRes.InviteState.Events...)
	}
	if entry.membershipEventID != "" && (entry.inviteEvent == nil || entry.membershipEventID != entry.inviteEvent.EventID()) {
		// Include the user's own membership event, e.g. the knock or the rejection of an invite.
		ev, err := r.snapshot.GetStateEvent(r.ctx, entry.roomID, spec.MRoomMember, r.device.UserID)
		if err != nil {
			return nil, fmt.Errorf("snapshot.GetStateEvent: %w", err)
		}
		if ev != nil && ev.EventID() == entry.membershipEventID {
			stripped, err := r.strippedEvent(ev)
			if err != nil {
				return nil, err
			}
			strippedState = append(strippedState, stripped)
		}
	}
	return strippedState, nil
}

// strippedEvent converts an event into the stripped state format.
func (r *slidingSyncRequest) strippedEvent(ev *rstypes.HeaderedEvent) (json.RawMessage, error) {
	clientEvent, err := synctypes.ToClientEvent(ev, synctypes.FormatSync, r.userIDForSender)
	if err != nil {
		return nil, fmt.Errorf("synctypes.ToClientEvent: %w", err)
	}
	return json.Marshal(struct {
		Content  spec.RawJSON `json:"content"`
		Sender   string       `json:"sender"`
		StateKey *string      `json:"state_key"`
		Type     string       `json:"type"`
	}{clientEvent.Content, clientEvent.Sender, clientEvent.StateKey, clientEvent.Type})
}

// fullRoomResult returns the result for a room the user is or has been joined to. If prev
// is nil, the full room data is returned, otherwise only what changed since prev.
// nolint:gocyclo
func (r *slidingSyncRequest) fullRoomResult(
	entry *slidingSyncRoomEntry, cfg *slidingSyncRoomConfig, prev *slidingSyncRoomState, isDM bool,
) (*types.SlidingSyncRoom, *slidingSyncRoomState, error) {
	initial := prev == nil
	// The position up to which the user may see the room.
	viewPos := r.currentPos
	latestPos := entry.sortPos
	if entry.membership != spec.Join {
		viewPos = entry.membershipPos
		latestPos = entry.membershipPos
	}
	lazy := lazyMembers(cfg.requiredState)

	var (
		from                 types.StreamPosition
		hasNewEvents         bool
		expandTimeline       bool
		requiredStateChanged bool
	)
	if !initial {
		from = prev.LastPos
		hasNewEvents = latestPos > prev.LastPos
		expandTimeline = prev.TimelineKnown >= 0 && cfg.timelineLimit > prev.TimelineKnown
		requiredStateChanged = !requiredStateConfigsEqual(cfg.requiredState, prev.RequiredState)
		if viewPos < from {
			viewPos = from
		}
	}

	newState := &slidingSyncRoomState{
		Membership:    entry.membership,
		LastPos:       viewPos,
		RequiredState: cfg.requiredState,
	}
	if !initial {
		// Start from the previous values, and update them with what is sent below.
		*newState = *prev
		newState.Membership = entry.membership
		newState.LastPos = viewPos
		newState.RequiredState = cfg.requiredState
		membershipChanged := prev.Membership != entry.membership
		if !hasNewEvents && !membershipChanged && !expandTimeline && !requiredStateChanged && prev.IsDM == isDM {
			return nil, newState, nil
		}
	}

	res := &types.SlidingSyncRoom{
		Membership: entry.membership,
		Lists:      cfg.lists,
		Initial:    initial,
	}
	source := newSlidingSyncStateSource(r.ctx, r.snapshot, r.rp.rsAPI, entry)

	// Timeline
	var timeline []*rstypes.HeaderedEvent
	var limited bool
	if initial || hasNewEvents || expandTimeline {
		timelineFrom := from
		if initial || expandTimeline {
			timelineFrom = 0
		}
		events, positions, rawLimited, err := r.timeline(entry.roomID, timelineFrom, viewPos, cfg.timelineLimit)
		if err != nil {
			return nil, nil, err
		}
		timeline = events
		limited = rawLimited
		numLive := 0
		for _, ev := range events {
			pos := positions[ev.EventID()]
			if !r.base.token.IsEmpty() && pos > r.base.token.PDUPosition {
				numLive++
			}
			if !initial && pos <= from {
				res.ExpandedTimeline = true
			}
		}
		switch {
		case rawLimited:
			newState.TimelineKnown = len(events)
		case initial || expandTimeline || prev.TimelineKnown < 0:
			newState.TimelineKnown = -1
		default:
			newState.TimelineKnown = min(prev.TimelineKnown+len(events), types.SlidingSyncMaxTimelineLimit)
		}
		if expandTimeline && !res.ExpandedTimeline && !hasNewEvents {
			// The client already had all the events.
			timeline = nil
		}
		if len(timeline) > 0 {
			res.Timeline = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(timeline), synctypes.FormatSync, r.userIDForSender)
			res.NumLive = &numLive
			res.Limited = limited
			prevBatch, err := r.prevBatch(timeline)
			if err != nil {
				return nil, nil, err
			}
			res.PrevBatch = prevBatch
		}
	}

	// Required state
	state := newSlidingSyncStateCollector()
	if initial {
		events, err := source.matching(cfg.requiredState)
		if err != nil {
			return nil, nil, err
		}
		state.addEvents(events)
	} else {
		if hasNewEvents {
			changed, err := r.snapshot.StateTuplesChangedInRange(r.ctx, []string{entry.roomID}, types.Range{From: from, To: viewPos})
			if err != nil {
				return nil, nil, fmt.Errorf("snapshot.StateTuplesChangedInRange: %w", err)
			}
			for _, tuple := range changed[entry.roomID] {
				include := includesState(cfg.requiredState, tuple.EventType, tuple.StateKey)
				// Membership changes are sent for lazily loaded members if the timeline isn't
				// limited, so clients can keep their membership cache up to date.
				include = include || (lazy && !limited && tuple.EventType == spec.MRoomMember)
				if !include {
					continue
				}
				ev, err := source.get(tuple.EventType, tuple.StateKey)
				if err != nil {
					return nil, nil, err
				}
				if ev != nil {
					state.addEvents([]*rstypes.HeaderedEvent{ev})
				} else {
					state.addStub(tuple)
				}
				if lazy && tuple.EventType == spec.MRoomMember {
					newState.LazyMembers = withLazyMember(newState.LazyMembers, tuple.StateKey)
				}
			}
		}
		if requiredStateChanged {
			// Send the state which matches the new config, but didn't match the old one.
			events, err := source.matching(cfg.requiredState)
			if err != nil {
				return nil, nil, err
			}
			for _, ev := range events {
				if !includesState(prev.RequiredState, ev.Type(), *ev.StateKey()) {
					state.addEvents([]*rstypes.HeaderedEvent{ev})
				}
			}
		}
	}
	if lazy {
		if initial || limited || res.ExpandedTimeline {
			// Clients flush their membership cache in these cases.
			newState.LazyMembers = nil
		}
		if err := r.lazyLoadMembers(source, timeline, state, newState); err != nil {
			return nil, nil, err
		}
	} else {
		newState.LazyMembers = nil
	}
	requiredState, err := state.toJSON(r.userIDForSender)
	if err != nil {
		return nil, nil, err
	}
	res.RequiredState = requiredState

	// Room summary
	if err = r.summary(entry, source, res, newState, prev); err != nil {
		return nil, nil, err
	}
	if initial || prev.IsDM != isDM {
		if isDM || !initial {
			res.IsDM = &isDM
		}
		newState.IsDM = isDM
	}

	// Don't send anything if nothing changed, e.g. when the required state changed
	// without matching any new state.
	if !initial && len(res.Timeline) == 0 && len(res.RequiredState) == 0 && res.Name == nil &&
		res.Avatar == nil && res.Heroes == nil && res.IsDM == nil && res.BumpStamp == nil &&
		res.JoinedCount == nil && res.InvitedCount == nil && prev.Membership == entry.membership {
		return nil, newState, nil
	}
	if !initial && prev.Membership == entry.membership {
		res.Membership = ""
	}
	return res, newState, nil
}

// timeline returns the latest events in the room within (from, to], up to limit events,
// filtered by history visibility and ignored users. It also returns the stream positions
// of the events, and whether there are more events in the range.
func (r *slidingSyncRequest) timeline(
	roomID string, from, to types.StreamPosition, limit int,
) ([]*rstypes.HeaderedEvent, map[string]types.StreamPosition, bool, error) {
	if to <= from {
		return nil, nil, false, nil
	}
	if limit <= 0 {
		// There may be events, but the client doesn't want any.
		return nil, nil, true, nil
	}
	filter := synctypes.DefaultRoomEventFilter()
	filter.Limit = limit
	if len(r.ignored) > 0 {
		filter.NotSenders = &r.ignored
	}
	recent, err := r.snapshot.RecentEvents(r.ctx, []string{roomID}, types.Range{From: from, To: to}, &filter, true, true)
	if err != nil {
		return nil, nil, false, fmt.Errorf("snapshot.RecentEvents: %w", err)
	}
	streamEvents := recent[roomID].Events
	positions := make(map[string]types.StreamPosition, len(streamEvents))
	for _, ev := range streamEvents {
		positions[ev.EventID()] = ev.StreamPosition
	}
	events := r.snapshot.StreamEventsToEvents(r.ctx, r.device, streamEvents, r.rp.rsAPI)
	filtered, err := streams.ApplyHistoryVisibilityFilter(r.ctx, r.snapshot, r.rp.rsAPI, roomID, r.device.UserID, events)
	if err != nil {
		return nil, nil, false, fmt.Errorf("ApplyHistoryVisibilityFilter: %w", err)
	}
	// If the history visibility filter removed events, the client can't paginate them either.
	limited := recent[roomID].Limited && len(filtered) == len(events)
	return filtered, positions, limited, nil
}

// prevBatch returns a token which can be used to paginate backwards from the given events.
func (r *slidingSyncRequest) prevBatch(events []*rstypes.HeaderedEvent) (string, error) {
	event := events[0]
	// If this is the beginning of the room, we can't go back further.
	if event.Type() == spec.MRoomCreate && event.StateKeyEquals("") {
		event = events[len(events)-1]
	}
	topoPos, streamPos, err := r.snapshot.PositionInTopology(r.ctx, event.EventID())
	if err != nil {
		return "", fmt.Errorf("snapshot.PositionInTopology: %w", err)
	}
	token := types.TopologyToken{Depth: topoPos, PDUPosition: streamPos}
	token.Decrement()
	return token.String(), nil
}

// lazyLoadMembers adds the membership events of the senders and targets of the timeline
// events, which have not been sent to the client yet.
func (r *slidingSyncRequest) lazyLoadMembers(
	source *slidingSyncStateSource, timeline []*rstypes.HeaderedEvent,
	state *slidingSyncStateCollector, newState *slidingSyncRoomState,
) error {
	var wanted []string
	seen := make(map[string]struct{})
	addWanted := func(stateKey string) {
		if _, ok := seen[stateKey]; ok {
			return
		}
		seen[stateKey] = struct{}{}
		if _, ok := newState.LazyMembers[stateKey]; !ok {
			wanted = append(wanted, stateKey)
		}
	}
	for _, ev := range timeline {
		addWanted(string(ev.SenderID()))
		if ev.Type() == spec.MRoomMember && ev.StateKey() != nil {
			addWanted(*ev.StateKey())
		}
	}
	for _, stateKey := range wanted {
		ev, err := source.get(spec.MRoomMember, stateKey)
		if err != nil {
			return err
		}
		if ev != nil {
			state.addEvents([]*rstypes.HeaderedEvent{ev})
		}
		newState.LazyMembers = withLazyMember(newState.LazyMembers, stateKey)
	}
	return nil
}

// withLazyMember returns a copy of members with the given member added, as room states are immutable.
func withLazyMember(members map[string]struct{}, stateKey string) map[string]struct{} {
	if _, ok := members[stateKey]; ok {
		return members
	}
	if len(members) >= slidingSyncMaxLazyMembers {
		members = nil
	}
	result := make(map[string]struct{}, len(members)+1)
	maps.Copy(result, members)
	result[stateKey] = struct{}{}
	return result
}

// summary adds the name, avatar, heroes, member counts and bump stamp to the result,
// if they changed since they were last sent.
// nolint:gocyclo
func (r *slidingSyncRequest) summary(
	entry *slidingSyncRoomEntry, source *slidingSyncStateSource,
	res *types.SlidingSyncRoom, newState *slidingSyncRoomState, prev *slidingSyncRoomState,
) error {
	initial := prev == nil

	// Name and avatar
	name, err := stateContentString(source, spec.MRoomName, "name")
	if err != nil {
		return err
	}
	if initial || !optionalStringEqual(prev.Name, name) {
		if name != nil || !initial {
			res.Name = &types.NullableString{Value: name}
		}
		newState.Name = name
	}
	avatar, err := stateContentString(source, spec.MRoomAvatar, "url")
	if err != nil {
		return err
	}
	if initial || !optionalStringEqual(prev.Avatar, avatar) {
		if avatar != nil || !initial {
			res.Avatar = &types.NullableString{Value: avatar}
		}
		newState.Avatar = avatar
	}

	// Bump stamp
	bumpStamp := int64(entry.membershipPos)
	if entry.membership == spec.Join {
		bumpStamp = int64(entry.sortPos)
		if pos, ok := r.bumpStamps[entry.roomID]; ok {
			bumpStamp = int64(pos)
		}
	}
	if initial || prev.BumpStamp != bumpStamp {
		res.BumpStamp = &bumpStamp
		newState.BumpStamp = bumpStamp
	}

	// The member counts and heroes are only available for joined rooms, as the summary
	// is calculated from the current state.
	if entry.membership != spec.Join {
		return nil
	}
	summary, err := r.snapshot.GetRoomSummary(r.ctx, entry.roomID, r.device.UserID)
	if err != nil {
		return fmt.Errorf("snapshot.GetRoomSummary: %w", err)
	}
	if summary.JoinedMemberCount != nil && (initial || prev.JoinedCount != *summary.JoinedMemberCount) {
		res.JoinedCount = summary.JoinedMemberCount
		newState.JoinedCount = *summary.JoinedMemberCount
	}
	if summary.InvitedMemberCount != nil && (initial || prev.InvitedCount != *summary.InvitedMemberCount) {
		res.InvitedCount = summary.InvitedMemberCount
		newState.InvitedCount = *summary.InvitedMemberCount
	}
	var heroes []types.SlidingSyncHero
	if name == nil {
		for _, heroID := range summary.Heroes {
			hero := types.SlidingSyncHero{UserID: heroID}
			ev, err := source.get(spec.MRoomMember, heroID)
			if err != nil {
				return err
			}
			if ev != nil {
				hero.DisplayName = gjson.GetBytes(ev.Content(), "displayname").Str
				hero.AvatarURL = gjson.GetBytes(ev.Content(), "avatar_url").Str
				if userID, err := r.userIDForSender(ev.RoomID(), spec.SenderID(heroID)); err == nil && userID != nil {
					hero.UserID = userID.String()
				}
			}
			heroes = append(heroes, hero)
		}
	}
	heroesKey := heroesKey(heroes)
	if (initial && len(heroes) > 0) || (!initial && prev.HeroesKey != heroesKey && len(heroes) > 0) {
		res.Heroes = heroes
	}
	newState.HeroesKey = heroesKey
	return nil
}

func heroesKey(heroes []types.SlidingSyncHero) string {
	var sb strings.Builder
	for _, hero := range heroes {
		sb.WriteString(hero.UserID)
		sb.WriteByte(0)
		sb.WriteString(hero.DisplayName)
		sb.WriteByte(0)
		sb.WriteString(hero.AvatarURL)
		sb.WriteByte(0)
	}
	return sb.String()
}

// stateContentString returns a string field of the content of a state event, or nil
// if the state event doesn't exist or the field is empty.
func stateContentString(source *slidingSyncStateSource, eventType, field string) (*string, error) {
	ev, err := source.get(eventType, "")
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return nil, nil
	}
	value := gjson.GetBytes(ev.Content(), field)
	if value.Type != gjson.String || value.Str == "" {
		return nil, nil
	}
	return &value.Str, nil
}

// slidingSyncStateCollector collects the required state of a room, deduplicated by state tuple.
type slidingSyncStateCollector struct {
	order  []gomatrixserverlib.StateKeyTuple
	events map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent
}

func newSlidingSyncStateCollector() *slidingSyncStateCollector {
	return &slidingSyncStateCollector{events: make(map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent)}
}

func (c *slidingSyncStateCollector) add(tuple gomatrixserverlib.StateKeyTuple, ev *rstypes.HeaderedEvent) {
	if _, ok := c.events[tuple]; !ok {
		c.order = append(c.order, tuple)
	}
	c.events[tuple] = ev
}

func (c *slidingSyncStateCollector) addEvents(events []*rstypes.HeaderedEvent) {
	for _, ev := range events {
		if ev == nil || ev.StateKey() == nil {
			continue
		}
		c.add(gomatrixserverlib.StateKeyTuple{EventType: ev.Type(), StateKey: *ev.StateKey()}, ev)
	}
}

// addStub records that the given state has been removed.
func (c *slidingSyncStateCollector) addStub(tuple gomatrixserverlib.StateKeyTuple) {
	c.add(tuple, nil)
}

func (c *slidingSyncStateCollector) toJSON(userIDForSender spec.UserIDForSender) ([]json.RawMessage, error) {
	result := make([]json.RawMessage, 0, len(c.order))
	for _, tuple := range c.order {
		var data []byte
		var err error
		if ev := c.events[tuple]; ev != nil {
			var clientEvent *synctypes.ClientEvent
			clientEvent, err = synctypes.ToClientEvent(ev, synctypes.FormatSync, userIDForSender)
			if err != nil {
				return nil, fmt.Errorf("synctypes.ToClientEvent: %w", err)
			}
			data, err = json.Marshal(clientEvent)
		} else {
			data, err = json.Marshal(types.SlidingSyncStateStub{Type: tuple.EventType, StateKey: tuple.StateKey})
		}
		if err != nil {
			return nil, err
		}
		result = append(result, data)
	}
	return result, nil
}
