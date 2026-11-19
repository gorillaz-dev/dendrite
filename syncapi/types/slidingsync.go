// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/element-hq/dendrite/syncapi/synctypes"
)

// This file contains the request and response types for simplified sliding
// sync, as defined by MSC4186:
// https://github.com/matrix-org/matrix-spec-proposals/pull/4186

const (
	// SlidingSyncMaxLists is the maximum number of lists a request may contain.
	SlidingSyncMaxLists = 100
	// SlidingSyncMaxRoomSubscriptions is the maximum number of room subscriptions a request may contain.
	SlidingSyncMaxRoomSubscriptions = 100
	// SlidingSyncMaxRequiredStateElements is the maximum number of elements in each of the
	// `include` and `exclude` fields of a `required_state` request.
	SlidingSyncMaxRequiredStateElements = 100
	// SlidingSyncMaxFilterValues is the maximum number of values in a list filter field
	// such as `spaces` or `tags`.
	SlidingSyncMaxFilterValues = 100
	// SlidingSyncMaxConnIDLength is the maximum length of a `conn_id`.
	SlidingSyncMaxConnIDLength = 255
	// SlidingSyncMaxTimelineLimit is the maximum number of timeline events which are returned per room.
	// Clients asking for more get this many events.
	SlidingSyncMaxTimelineLimit = 100
)

// SlidingSyncRequest is the body of a sliding sync request.
type SlidingSyncRequest struct {
	ConnID            string                                 `json:"conn_id,omitempty"`
	Pos               string                                 `json:"pos,omitempty"`
	Timeout           int64                                  `json:"timeout,omitempty"`
	SetPresence       string                                 `json:"set_presence,omitempty"`
	Lists             map[string]SlidingSyncListConfig       `json:"lists,omitempty"`
	RoomSubscriptions map[string]SlidingSyncRoomSubscription `json:"room_subscriptions,omitempty"`
	// Extensions are not specified by MSC4186 itself. They are accepted but ignored.
	Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

// SlidingSyncListConfig describes a list of rooms the client is interested in.
type SlidingSyncListConfig struct {
	TimelineLimit *int                      `json:"timeline_limit"`
	RequiredState *SlidingSyncRequiredState `json:"required_state"`
	Range         []int64                   `json:"range,omitempty"`
	Filters       *SlidingSyncRoomFilter    `json:"filters,omitempty"`
}

// SlidingSyncRoomSubscription describes a single room the client is interested in.
type SlidingSyncRoomSubscription struct {
	TimelineLimit *int                      `json:"timeline_limit"`
	RequiredState *SlidingSyncRequiredState `json:"required_state"`
}

// SlidingSyncRequiredState describes which state the server should return for a room.
type SlidingSyncRequiredState struct {
	Include     []SlidingSyncRequiredStateElement `json:"include,omitempty"`
	Exclude     []SlidingSyncRequiredStateElement `json:"exclude,omitempty"`
	LazyMembers bool                              `json:"lazy_members,omitempty"`
}

// SlidingSyncRequiredStateElement matches state events. A nil field matches all values.
type SlidingSyncRequiredStateElement struct {
	Type     *string `json:"type,omitempty"`
	StateKey *string `json:"state_key,omitempty"`
}

// Matches returns true if the element matches the given event type and state key.
func (e SlidingSyncRequiredStateElement) Matches(eventType, stateKey string) bool {
	if e.Type != nil && *e.Type != eventType {
		return false
	}
	if e.StateKey != nil && *e.StateKey != stateKey {
		return false
	}
	return true
}

// SlidingSyncRoomFilter filters the rooms of a list.
type SlidingSyncRoomFilter struct {
	IsDM         *bool     `json:"is_dm,omitempty"`
	Spaces       []string  `json:"spaces,omitempty"`
	IsEncrypted  *bool     `json:"is_encrypted,omitempty"`
	IsInvited    *bool     `json:"is_invited,omitempty"`
	RoomTypes    []*string `json:"room_types,omitempty"`
	NotRoomTypes []*string `json:"not_room_types,omitempty"`
	Tags         []string  `json:"tags,omitempty"`
	NotTags      []string  `json:"not_tags,omitempty"`
}

// Validate checks the request for invalid values and enforces the limits
// on the request size. The returned error message is safe to return to clients.
func (r *SlidingSyncRequest) Validate() error {
	if len(r.ConnID) > SlidingSyncMaxConnIDLength {
		return fmt.Errorf("conn_id must not be longer than %d characters", SlidingSyncMaxConnIDLength)
	}
	if r.Timeout < 0 {
		return fmt.Errorf("timeout must not be negative")
	}
	switch r.SetPresence {
	case "", "online", "offline", "unavailable":
	default:
		return fmt.Errorf("unknown set_presence value")
	}
	if len(r.Lists) > SlidingSyncMaxLists {
		return fmt.Errorf("too many lists, the maximum is %d", SlidingSyncMaxLists)
	}
	if len(r.RoomSubscriptions) > SlidingSyncMaxRoomSubscriptions {
		return fmt.Errorf("too many room subscriptions, the maximum is %d", SlidingSyncMaxRoomSubscriptions)
	}
	for key, list := range r.Lists {
		if !isOpaqueIdentifier(key) {
			return fmt.Errorf("list key %q is not a valid opaque identifier", key)
		}
		if err := validateRoomConfig(list.TimelineLimit, list.RequiredState); err != nil {
			return fmt.Errorf("list %q: %w", key, err)
		}
		if list.Range != nil {
			if len(list.Range) != 2 {
				return fmt.Errorf("list %q: range must contain exactly two integers", key)
			}
			if list.Range[0] < 0 || list.Range[1] < list.Range[0] {
				return fmt.Errorf("list %q: invalid range", key)
			}
		}
		if err := list.Filters.validate(); err != nil {
			return fmt.Errorf("list %q: %w", key, err)
		}
	}
	for roomID, sub := range r.RoomSubscriptions {
		if _, err := spec.NewRoomID(roomID); err != nil {
			return fmt.Errorf("room subscription key %q is not a valid room ID", roomID)
		}
		if err := validateRoomConfig(sub.TimelineLimit, sub.RequiredState); err != nil {
			return fmt.Errorf("room subscription %q: %w", roomID, err)
		}
	}
	return nil
}

func validateRoomConfig(timelineLimit *int, requiredState *SlidingSyncRequiredState) error {
	if timelineLimit == nil {
		return fmt.Errorf("missing timeline_limit")
	}
	if *timelineLimit < 0 {
		return fmt.Errorf("timeline_limit must not be negative")
	}
	if requiredState == nil {
		return fmt.Errorf("missing required_state")
	}
	if len(requiredState.Include) > SlidingSyncMaxRequiredStateElements ||
		len(requiredState.Exclude) > SlidingSyncMaxRequiredStateElements {
		return fmt.Errorf("too many required_state elements, the maximum is %d", SlidingSyncMaxRequiredStateElements)
	}
	return nil
}

func (f *SlidingSyncRoomFilter) validate() error {
	if f == nil {
		return nil
	}
	if len(f.Spaces) > SlidingSyncMaxFilterValues || len(f.RoomTypes) > SlidingSyncMaxFilterValues ||
		len(f.NotRoomTypes) > SlidingSyncMaxFilterValues || len(f.Tags) > SlidingSyncMaxFilterValues ||
		len(f.NotTags) > SlidingSyncMaxFilterValues {
		return fmt.Errorf("too many filter values, the maximum is %d", SlidingSyncMaxFilterValues)
	}
	for _, spaceID := range f.Spaces {
		if _, err := spec.NewRoomID(spaceID); err != nil {
			return fmt.Errorf("space %q is not a valid room ID", spaceID)
		}
	}
	return nil
}

// isOpaqueIdentifier checks that the given string follows the grammar for
// opaque identifiers: 1-255 characters from [0-9A-Za-z-._~].
func isOpaqueIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 255 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c == '-', c == '.', c == '_', c == '~':
		default:
			return false
		}
	}
	return true
}

// SlidingSyncPosition is the `pos` token of a sliding sync connection. It consists of
// a connection specific position, which identifies the state of the connection, and the
// stream token the response was calculated at. The stream token part allows the position
// to be used as a pagination token in `/messages`.
type SlidingSyncPosition struct {
	ConnectionPosition int64
	StreamToken        StreamingToken
}

// String returns the serialised form of the position, e.g. "5/s1_2_3_4_5_6_7_8_9".
func (p SlidingSyncPosition) String() string {
	return strconv.FormatInt(p.ConnectionPosition, 10) + "/" + p.StreamToken.String()
}

// NewSlidingSyncPositionFromString parses a position returned by String.
func NewSlidingSyncPositionFromString(s string) (SlidingSyncPosition, error) {
	connPos, streamToken, ok := strings.Cut(s, "/")
	if !ok {
		return SlidingSyncPosition{}, ErrMalformedSyncToken
	}
	pos, err := strconv.ParseInt(connPos, 10, 64)
	if err != nil || pos < 0 {
		return SlidingSyncPosition{}, ErrMalformedSyncToken
	}
	token, err := NewStreamTokenFromString(streamToken)
	if err != nil {
		return SlidingSyncPosition{}, ErrMalformedSyncToken
	}
	return SlidingSyncPosition{ConnectionPosition: pos, StreamToken: token}, nil
}

// SlidingSyncResponse is the body of a sliding sync response.
type SlidingSyncResponse struct {
	Pos        string                           `json:"pos"`
	Lists      map[string]SlidingSyncListResult `json:"lists"`
	Rooms      map[string]*SlidingSyncRoom      `json:"rooms"`
	Extensions map[string]json.RawMessage       `json:"extensions"`
}

// NewSlidingSyncResponse creates an empty response with initialised maps.
func NewSlidingSyncResponse() *SlidingSyncResponse {
	return &SlidingSyncResponse{
		Lists:      map[string]SlidingSyncListResult{},
		Rooms:      map[string]*SlidingSyncRoom{},
		Extensions: map[string]json.RawMessage{},
	}
}

// SlidingSyncListResult is the result of a list.
type SlidingSyncListResult struct {
	Count int `json:"count"`
}

// SlidingSyncRoom is the result for a single room. All fields are optional:
// if Initial is false an omitted field means that the value did not change.
type SlidingSyncRoom struct {
	// Common fields
	BumpStamp  *int64   `json:"bump_stamp,omitempty"`
	Membership string   `json:"membership,omitempty"`
	Lists      []string `json:"lists,omitempty"`

	// Fields for rooms the user is or has been joined to
	Name             *NullableString         `json:"name,omitempty"`
	Avatar           *NullableString         `json:"avatar,omitempty"`
	Heroes           []SlidingSyncHero       `json:"heroes,omitempty"`
	IsDM             *bool                   `json:"is_dm,omitempty"`
	Initial          bool                    `json:"initial,omitempty"`
	ExpandedTimeline bool                    `json:"expanded_timeline,omitempty"`
	RequiredState    []json.RawMessage       `json:"required_state,omitempty"`
	Timeline         []synctypes.ClientEvent `json:"timeline,omitempty"`
	PrevBatch        string                  `json:"prev_batch,omitempty"`
	Limited          bool                    `json:"limited,omitempty"`
	NumLive          *int                    `json:"num_live,omitempty"`
	JoinedCount      *int                    `json:"joined_count,omitempty"`
	InvitedCount     *int                    `json:"invited_count,omitempty"`

	// Fields for rooms the user was invited to or knocked on, but never joined.
	StrippedState []json.RawMessage `json:"stripped_state,omitempty"`
}

// SlidingSyncHero is a member of a room which can be used to calculate the room name.
type SlidingSyncHero struct {
	UserID      string `json:"user_id"`
	DisplayName string `json:"displayname,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
}

// SlidingSyncStateStub indicates that a piece of state has been removed.
type SlidingSyncStateStub struct {
	Type     string `json:"type"`
	StateKey string `json:"state_key"`
}

// NullableString is a string which is serialised as JSON null if Value is nil.
type NullableString struct {
	Value *string
}

// MarshalJSON implements json.Marshaler.
func (n NullableString) MarshalJSON() ([]byte, error) {
	if n.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*n.Value)
}

// UnmarshalJSON implements json.Unmarshaler.
func (n *NullableString) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Value = nil
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	n.Value = &s
	return nil
}
