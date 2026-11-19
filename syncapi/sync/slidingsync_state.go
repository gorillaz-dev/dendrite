// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"fmt"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"

	roomserverAPI "github.com/element-hq/dendrite/roomserver/api"
	rstypes "github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/synctypes"
	"github.com/element-hq/dendrite/syncapi/types"
)

// slidingSyncStateSource provides the room state the user is allowed to see: the current
// state for joined rooms, and the state at the time the user left for all other rooms.
type slidingSyncStateSource struct {
	ctx      context.Context
	snapshot storage.DatabaseTransaction
	rsAPI    roomserverAPI.SyncRoomserverAPI
	roomID   string
	// The event after which the state is used, for rooms the user is no longer joined to.
	// Empty for joined rooms.
	atEventID string

	loaded     bool
	stateAfter map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent
}

func newSlidingSyncStateSource(
	ctx context.Context, snapshot storage.DatabaseTransaction, rsAPI roomserverAPI.SyncRoomserverAPI, entry *slidingSyncRoomEntry,
) *slidingSyncStateSource {
	s := &slidingSyncStateSource{ctx: ctx, snapshot: snapshot, rsAPI: rsAPI, roomID: entry.roomID}
	if entry.membership != spec.Join {
		s.atEventID = entry.membershipEventID
	}
	return s
}

// loadStateAfter loads the full room state after the user's membership event.
func (s *slidingSyncStateSource) loadStateAfter() error {
	if s.loaded {
		return nil
	}
	s.stateAfter = make(map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent)
	if s.atEventID == "" {
		// Without a membership event we cannot know what the user may see.
		s.loaded = true
		return nil
	}
	res := roomserverAPI.QueryStateAfterEventsResponse{}
	if err := s.rsAPI.QueryStateAfterEvents(s.ctx, &roomserverAPI.QueryStateAfterEventsRequest{
		RoomID:       s.roomID,
		PrevEventIDs: []string{s.atEventID},
	}, &res); err != nil {
		return fmt.Errorf("rsAPI.QueryStateAfterEvents: %w", err)
	}
	for _, ev := range res.StateEvents {
		if ev.StateKey() == nil {
			continue
		}
		s.stateAfter[gomatrixserverlib.StateKeyTuple{EventType: ev.Type(), StateKey: *ev.StateKey()}] = ev
	}
	s.loaded = true
	return nil
}

// get returns the state event for the given tuple, or nil if there is none.
func (s *slidingSyncStateSource) get(eventType, stateKey string) (*rstypes.HeaderedEvent, error) {
	if s.atEventID == "" {
		return s.snapshot.GetStateEvent(s.ctx, s.roomID, eventType, stateKey)
	}
	if err := s.loadStateAfter(); err != nil {
		return nil, err
	}
	return s.stateAfter[gomatrixserverlib.StateKeyTuple{EventType: eventType, StateKey: stateKey}], nil
}

// matching returns all state events matching the given required state configs
// (without taking lazily loaded members into account).
func (s *slidingSyncStateSource) matching(configs []types.SlidingSyncRequiredState) ([]*rstypes.HeaderedEvent, error) {
	var candidates []*rstypes.HeaderedEvent
	if s.atEventID != "" {
		if err := s.loadStateAfter(); err != nil {
			return nil, err
		}
		for _, ev := range s.stateAfter {
			candidates = append(candidates, ev)
		}
	} else {
		var err error
		if candidates, err = s.currentStateCandidates(configs); err != nil {
			return nil, err
		}
	}
	result := make([]*rstypes.HeaderedEvent, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, ev := range candidates {
		if ev == nil || ev.StateKey() == nil {
			continue
		}
		if _, ok := seen[ev.EventID()]; ok {
			continue
		}
		if includesState(configs, ev.Type(), *ev.StateKey()) {
			seen[ev.EventID()] = struct{}{}
			result = append(result, ev)
		}
	}
	return result, nil
}

// currentStateCandidates fetches a superset of the current state matching the configs, while
// avoiding loading all members of a room unless they are actually required.
func (s *slidingSyncStateSource) currentStateCandidates(configs []types.SlidingSyncRequiredState) ([]*rstypes.HeaderedEvent, error) {
	var (
		wildcardType bool
		allMembers   bool
		eventTypes   []string
		memberKeys   []string
	)
	for _, rs := range configs {
		for _, inc := range rs.Include {
			switch {
			case inc.Type == nil:
				wildcardType = true
				if inc.StateKey == nil && !rs.LazyMembers {
					allMembers = true
				} else if inc.StateKey != nil {
					memberKeys = append(memberKeys, *inc.StateKey)
				}
			case *inc.Type == spec.MRoomMember:
				if inc.StateKey != nil {
					memberKeys = append(memberKeys, *inc.StateKey)
				} else if !rs.LazyMembers {
					allMembers = true
				}
			default:
				eventTypes = append(eventTypes, *inc.Type)
			}
		}
	}

	var candidates []*rstypes.HeaderedEvent
	filter := synctypes.DefaultStateFilter()
	switch {
	case wildcardType:
		if !allMembers {
			filter.NotTypes = &[]string{spec.MRoomMember}
		}
	case allMembers:
		eventTypes = append(eventTypes, spec.MRoomMember)
		filter.Types = &eventTypes
	default:
		filter.Types = &eventTypes
	}
	if wildcardType || len(eventTypes) > 0 {
		events, err := s.snapshot.CurrentState(s.ctx, s.roomID, &filter, nil)
		if err != nil {
			return nil, fmt.Errorf("snapshot.CurrentState: %w", err)
		}
		candidates = append(candidates, events...)
	}
	if !allMembers {
		for _, stateKey := range memberKeys {
			ev, err := s.snapshot.GetStateEvent(s.ctx, s.roomID, spec.MRoomMember, stateKey)
			if err != nil {
				return nil, fmt.Errorf("snapshot.GetStateEvent: %w", err)
			}
			if ev != nil {
				candidates = append(candidates, ev)
			}
		}
	}
	return candidates, nil
}
