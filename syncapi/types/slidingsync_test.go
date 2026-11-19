package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestSlidingSyncRequestValidate(t *testing.T) {
	validList := `{"timeline_limit": 10, "required_state": {"include": [{}]}}`
	manyLists := make([]string, SlidingSyncMaxLists+1)
	for i := range manyLists {
		manyLists[i] = fmt.Sprintf(`"list%d": %s`, i, validList)
	}
	manySubs := make([]string, SlidingSyncMaxRoomSubscriptions+1)
	for i := range manySubs {
		manySubs[i] = fmt.Sprintf(`"!room%d:test": %s`, i, validList)
	}
	manyElements := `[` + strings.TrimSuffix(strings.Repeat(`{},`, SlidingSyncMaxRequiredStateElements+1), ",") + `]`

	testCases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "empty request", body: `{}`},
		{name: "full request", body: `{
			"conn_id": "main", "pos": "1/s1_0_0_0_0_0_0_0_0", "timeout": 1000, "set_presence": "offline",
			"lists": {"all-rooms_1.~": {"range": [0, 99], "timeline_limit": 10,
				"required_state": {"include": [{"type": "m.room.create", "state_key": ""}], "exclude": [{"type": "m.room.topic"}], "lazy_members": true},
				"filters": {"is_dm": true, "spaces": ["!space:test"], "room_types": [null, "m.space"], "tags": ["m.favourite"]}}},
			"room_subscriptions": {"!sub:test": {"timeline_limit": 5, "required_state": {}}},
			"extensions": {"e2ee": {"enabled": true}}
		}`},
		{name: "unknown presence", body: `{"set_presence": "busy"}`, wantErr: true},
		{name: "negative timeout", body: `{"timeout": -1}`, wantErr: true},
		{name: "conn_id too long", body: `{"conn_id": "` + strings.Repeat("a", SlidingSyncMaxConnIDLength+1) + `"}`, wantErr: true},
		{name: "too many lists", body: `{"lists": {` + strings.Join(manyLists, ",") + `}}`, wantErr: true},
		{name: "too many subscriptions", body: `{"room_subscriptions": {` + strings.Join(manySubs, ",") + `}}`, wantErr: true},
		{name: "invalid list key", body: `{"lists": {"a b": ` + validList + `}}`, wantErr: true},
		{name: "empty list key", body: `{"lists": {"": ` + validList + `}}`, wantErr: true},
		{name: "missing timeline_limit", body: `{"lists": {"a": {"required_state": {}}}}`, wantErr: true},
		{name: "negative timeline_limit", body: `{"lists": {"a": {"timeline_limit": -1, "required_state": {}}}}`, wantErr: true},
		{name: "missing required_state", body: `{"lists": {"a": {"timeline_limit": 1}}}`, wantErr: true},
		{name: "range with one value", body: `{"lists": {"a": {"timeline_limit": 1, "required_state": {}, "range": [0]}}}`, wantErr: true},
		{name: "negative range", body: `{"lists": {"a": {"timeline_limit": 1, "required_state": {}, "range": [-1, 5]}}}`, wantErr: true},
		{name: "inverted range", body: `{"lists": {"a": {"timeline_limit": 1, "required_state": {}, "range": [5, 1]}}}`, wantErr: true},
		{name: "too many required state elements", body: `{"lists": {"a": {"timeline_limit": 1, "required_state": {"include": ` + manyElements + `}}}}`, wantErr: true},
		{name: "invalid space", body: `{"lists": {"a": {"timeline_limit": 1, "required_state": {}, "filters": {"spaces": ["notaroom"]}}}}`, wantErr: true},
		{name: "invalid subscription room ID", body: `{"room_subscriptions": {"@user:test": ` + validList + `}}`, wantErr: true},
		{name: "subscription missing timeline_limit", body: `{"room_subscriptions": {"!room:test": {"required_state": {}}}}`, wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var req SlidingSyncRequest
			if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
				t.Fatalf("failed to unmarshal request: %s", err)
			}
			err := req.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected an error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

func TestSlidingSyncPosition(t *testing.T) {
	pos := SlidingSyncPosition{
		ConnectionPosition: 42,
		StreamToken:        StreamingToken{PDUPosition: 1, TypingPosition: 2, PresencePosition: 9},
	}
	parsed, err := NewSlidingSyncPositionFromString(pos.String())
	if err != nil {
		t.Fatalf("failed to parse position: %s", err)
	}
	if parsed != pos {
		t.Fatalf("got %+v, want %+v", parsed, pos)
	}

	for _, invalid := range []string{"", "42", "s1_2_3", "/s1_2_3", "x/s1_2_3", "-1/s1_2_3", "1/t1_2", "1/sfoo"} {
		if _, err = NewSlidingSyncPositionFromString(invalid); err == nil {
			t.Errorf("expected error parsing %q", invalid)
		}
	}
}

func TestSlidingSyncRequiredStateElementMatches(t *testing.T) {
	str := func(s string) *string { return &s }
	testCases := []struct {
		element  SlidingSyncRequiredStateElement
		evType   string
		stateKey string
		want     bool
	}{
		{SlidingSyncRequiredStateElement{}, "m.room.name", "", true},
		{SlidingSyncRequiredStateElement{Type: str("m.room.name")}, "m.room.name", "foo", true},
		{SlidingSyncRequiredStateElement{Type: str("m.room.name")}, "m.room.topic", "", false},
		{SlidingSyncRequiredStateElement{StateKey: str("@alice:test")}, "m.room.member", "@alice:test", true},
		{SlidingSyncRequiredStateElement{StateKey: str("@alice:test")}, "m.room.member", "@bob:test", false},
		{SlidingSyncRequiredStateElement{Type: str("m.room.member"), StateKey: str("")}, "m.room.member", "@bob:test", false},
	}
	for i, tc := range testCases {
		if got := tc.element.Matches(tc.evType, tc.stateKey); got != tc.want {
			t.Errorf("case %d: got %v want %v", i, got, tc.want)
		}
	}
}

func TestNullableString(t *testing.T) {
	name := "room"
	room := SlidingSyncRoom{Name: &NullableString{Value: &name}, Avatar: &NullableString{}}
	data, err := json.Marshal(room)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), `{"name":"room","avatar":null}`; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}
