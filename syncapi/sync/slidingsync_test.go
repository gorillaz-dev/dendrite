package sync

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/element-hq/dendrite/syncapi/types"
)

func TestSlidingSyncConnectionPositions(t *testing.T) {
	conns := newSlidingSyncConnections()
	conn, base := conns.create("@alice:test", "ALICE", "main")
	if len(base.rooms) != 0 {
		t.Fatalf("expected an empty initial snapshot")
	}
	token := types.StreamingToken{PDUPosition: 5}
	pos1 := conn.addSnapshot(token, map[string]*slidingSyncRoomState{"!a:test": {}}, nil)

	// The position can be used by the device it was issued to.
	gotConn, snap, err := conns.get("@alice:test", "ALICE", "main", pos1.String())
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if gotConn != conn || len(snap.rooms) != 1 {
		t.Fatalf("got the wrong connection or snapshot")
	}

	// The position must be rejected for other users, devices and connections.
	for _, tc := range []struct{ userID, deviceID, connID string }{
		{"@bob:test", "ALICE", "main"},
		{"@alice:test", "OTHER", "main"},
		{"@alice:test", "ALICE", "other"},
		{"@alice:test", "ALICE", ""},
	} {
		if _, _, err = conns.get(tc.userID, tc.deviceID, tc.connID, pos1.String()); err != errSlidingSyncUnknownPos {
			t.Errorf("%+v: expected errSlidingSyncUnknownPos, got %v", tc, err)
		}
	}

	// Forged and malformed positions must be rejected.
	forged := types.SlidingSyncPosition{ConnectionPosition: pos1.ConnectionPosition, StreamToken: types.StreamingToken{PDUPosition: 6}}
	for _, pos := range []string{forged.String(), "garbage", "999999/s1_0_0_0_0_0_0_0_0"} {
		if _, _, err = conns.get("@alice:test", "ALICE", "main", pos); err != errSlidingSyncUnknownPos {
			t.Errorf("%q: expected errSlidingSyncUnknownPos, got %v", pos, err)
		}
	}

	// Retrying with the same position works, and multiple responses may be created from it.
	pos2 := conn.addSnapshot(token, nil, nil)
	pos3 := conn.addSnapshot(token, nil, nil)
	if _, _, err = conns.get("@alice:test", "ALICE", "main", pos1.String()); err != nil {
		t.Fatalf("expected retrying with the same position to work: %s", err)
	}
	// Once a newer position is used, older positions are forgotten.
	if _, _, err = conns.get("@alice:test", "ALICE", "main", pos3.String()); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	for _, pos := range []types.SlidingSyncPosition{pos1, pos2} {
		if _, _, err = conns.get("@alice:test", "ALICE", "main", pos.String()); err != errSlidingSyncUnknownPos {
			t.Errorf("expected old position to be rejected, got %v", err)
		}
	}

	// Starting a new connection with the same conn_id invalidates the old positions.
	conns.create("@alice:test", "ALICE", "main")
	if _, _, err = conns.get("@alice:test", "ALICE", "main", pos3.String()); err != errSlidingSyncUnknownPos {
		t.Errorf("expected position of replaced connection to be rejected, got %v", err)
	}
}

func TestSlidingSyncConnectionSnapshotLimit(t *testing.T) {
	conns := newSlidingSyncConnections()
	conn, _ := conns.create("@alice:test", "ALICE", "")
	var positions []types.SlidingSyncPosition
	for i := 0; i < slidingSyncMaxSnapshotsPerConnection+3; i++ {
		positions = append(positions, conn.addSnapshot(types.StreamingToken{}, nil, nil))
	}
	if len(conn.snapshots) != slidingSyncMaxSnapshotsPerConnection {
		t.Fatalf("got %d snapshots, want %d", len(conn.snapshots), slidingSyncMaxSnapshotsPerConnection)
	}
	if _, _, err := conns.get("@alice:test", "ALICE", "", positions[0].String()); err != errSlidingSyncUnknownPos {
		t.Fatalf("expected oldest position to be dropped")
	}
	if _, _, err := conns.get("@alice:test", "ALICE", "", positions[len(positions)-1].String()); err != nil {
		t.Fatalf("expected newest position to be kept: %s", err)
	}
}

func TestSlidingSyncConnectionLimits(t *testing.T) {
	now := time.Now()
	conns := newSlidingSyncConnections()
	conns.now = func() time.Time { return now }

	// The number of connections per device is limited, the least recently used one is expired.
	var positions []types.SlidingSyncPosition
	for i := 0; i < slidingSyncMaxConnectionsPerDevice; i++ {
		now = now.Add(time.Second)
		conn, _ := conns.create("@alice:test", "ALICE", fmt.Sprintf("conn%d", i))
		positions = append(positions, conn.addSnapshot(types.StreamingToken{}, nil, nil))
	}
	// Use the first connection, so that the second one is the least recently used.
	now = now.Add(time.Second)
	if _, _, err := conns.get("@alice:test", "ALICE", "conn0", positions[0].String()); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	conns.create("@alice:test", "ALICE", "new")
	conns.create("@alice:test", "OTHERDEVICE", "new")
	if got, want := conns.count(), slidingSyncMaxConnectionsPerDevice+1; got != want {
		t.Fatalf("got %d connections, want %d", got, want)
	}
	if _, _, err := conns.get("@alice:test", "ALICE", "conn1", positions[1].String()); err != errSlidingSyncUnknownPos {
		t.Fatalf("expected least recently used connection to be expired")
	}
	if _, _, err := conns.get("@alice:test", "ALICE", "conn0", positions[0].String()); err != nil {
		t.Fatalf("expected recently used connection to be kept: %s", err)
	}

	// Unused connections expire.
	now = now.Add(slidingSyncConnectionTTL + time.Second)
	if _, _, err := conns.get("@alice:test", "ALICE", "conn0", positions[0].String()); err != errSlidingSyncUnknownPos {
		t.Fatalf("expected connection to be expired")
	}
	conns.expire()
	if got := conns.count(); got != 0 {
		t.Fatalf("expected all connections to be expired, got %d", got)
	}
}

func TestSlidingSyncRequiredState(t *testing.T) {
	str := func(s string) *string { return &s }
	all := types.SlidingSyncRequiredStateElement{}
	member := types.SlidingSyncRequiredStateElement{Type: str(spec.MRoomMember)}
	alice := types.SlidingSyncRequiredStateElement{Type: str(spec.MRoomMember), StateKey: str("@alice:test")}
	create := types.SlidingSyncRequiredStateElement{Type: str(spec.MRoomCreate), StateKey: str("")}

	testCases := []struct {
		name        string
		configs     []types.SlidingSyncRequiredState
		evType      string
		stateKey    string
		wantInclude bool
		wantLazy    bool
	}{
		{
			name:        "wildcard includes everything",
			configs:     []types.SlidingSyncRequiredState{{Include: []types.SlidingSyncRequiredStateElement{all}}},
			evType:      spec.MRoomMember,
			stateKey:    "@bob:test",
			wantInclude: true,
		},
		{
			name:     "nothing included by default",
			configs:  []types.SlidingSyncRequiredState{{}},
			evType:   spec.MRoomName,
			stateKey: "",
		},
		{
			name: "exclude takes priority",
			configs: []types.SlidingSyncRequiredState{{
				Include: []types.SlidingSyncRequiredStateElement{all},
				Exclude: []types.SlidingSyncRequiredStateElement{create},
			}},
			evType:   spec.MRoomCreate,
			stateKey: "",
		},
		{
			name: "union of configs",
			configs: []types.SlidingSyncRequiredState{
				{Include: []types.SlidingSyncRequiredStateElement{all}, Exclude: []types.SlidingSyncRequiredStateElement{create}},
				{Include: []types.SlidingSyncRequiredStateElement{create}},
			},
			evType:      spec.MRoomCreate,
			stateKey:    "",
			wantInclude: true,
		},
		{
			name:     "lazy members don't match wildcards",
			configs:  []types.SlidingSyncRequiredState{{Include: []types.SlidingSyncRequiredStateElement{all, member}, LazyMembers: true}},
			evType:   spec.MRoomMember,
			stateKey: "@bob:test",
			wantLazy: true,
		},
		{
			name:        "lazy members match explicit members",
			configs:     []types.SlidingSyncRequiredState{{Include: []types.SlidingSyncRequiredStateElement{all, alice}, LazyMembers: true}},
			evType:      spec.MRoomMember,
			stateKey:    "@alice:test",
			wantInclude: true,
			wantLazy:    true,
		},
		{
			name: "lazy loading is disabled if another config returns all members",
			configs: []types.SlidingSyncRequiredState{
				{Include: []types.SlidingSyncRequiredStateElement{all}, LazyMembers: true},
				{Include: []types.SlidingSyncRequiredStateElement{member}},
			},
			evType:      spec.MRoomMember,
			stateKey:    "@bob:test",
			wantInclude: true,
		},
		{
			name: "lazy loading stays enabled if another config returns some members",
			configs: []types.SlidingSyncRequiredState{
				{Include: []types.SlidingSyncRequiredStateElement{all}, LazyMembers: true},
				{Include: []types.SlidingSyncRequiredStateElement{alice}},
			},
			evType:   spec.MRoomMember,
			stateKey: "@bob:test",
			wantLazy: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := includesState(tc.configs, tc.evType, tc.stateKey); got != tc.wantInclude {
				t.Errorf("includesState: got %v want %v", got, tc.wantInclude)
			}
			if got := lazyMembers(tc.configs); got != tc.wantLazy {
				t.Errorf("lazyMembers: got %v want %v", got, tc.wantLazy)
			}
		})
	}
}

func TestSlidingSyncRoomConfigAdd(t *testing.T) {
	str := func(s string) *string { return &s }
	rsA := types.SlidingSyncRequiredState{Include: []types.SlidingSyncRequiredStateElement{{Type: str("a")}}}
	rsB := types.SlidingSyncRequiredState{Include: []types.SlidingSyncRequiredStateElement{{Type: str("b")}}}
	cfg := slidingSyncRoomConfig{}
	cfg.add(5, rsA)
	cfg.add(1, rsA)
	cfg.add(3, rsB)
	if cfg.timelineLimit != 5 {
		t.Errorf("got timeline limit %d, want 5", cfg.timelineLimit)
	}
	if len(cfg.requiredState) != 2 {
		t.Errorf("got %d required state configs, want 2", len(cfg.requiredState))
	}
	cfg.add(1000, rsB)
	if cfg.timelineLimit != types.SlidingSyncMaxTimelineLimit {
		t.Errorf("expected timeline limit to be capped, got %d", cfg.timelineLimit)
	}
}

func TestContainsRoomType(t *testing.T) {
	space := "m.space"
	other := "other"
	if !containsRoomType([]*string{nil}, nil) {
		t.Error("nil should match rooms without a type")
	}
	if containsRoomType([]*string{nil}, &space) {
		t.Error("nil should not match spaces")
	}
	if !containsRoomType([]*string{&other, &space}, &space) {
		t.Error("expected space to match")
	}
	if containsRoomType(nil, nil) {
		t.Error("empty list should not match")
	}
}

func TestWithLazyMember(t *testing.T) {
	members := withLazyMember(nil, "@alice:test")
	updated := withLazyMember(members, "@bob:test")
	if len(members) != 1 || len(updated) != 2 {
		t.Fatalf("expected the original map to be unmodified")
	}
	big := map[string]struct{}{}
	for i := 0; i < slidingSyncMaxLazyMembers; i++ {
		big[fmt.Sprintf("@user%d:test", i)] = struct{}{}
	}
	if reset := withLazyMember(big, "@new:test"); len(reset) != 1 {
		t.Fatalf("expected the tracked members to be reset, got %d", len(reset))
	}
}

func TestRequiredStateConfigsEqual(t *testing.T) {
	str := func(s string) *string { return &s }
	a := types.SlidingSyncRequiredState{Include: []types.SlidingSyncRequiredStateElement{{Type: str("a")}}}
	b := types.SlidingSyncRequiredState{Include: []types.SlidingSyncRequiredStateElement{{Type: str("b")}}, LazyMembers: true}
	if !requiredStateConfigsEqual([]types.SlidingSyncRequiredState{a, b}, []types.SlidingSyncRequiredState{b, a}) {
		t.Error("order must not matter")
	}
	if requiredStateConfigsEqual([]types.SlidingSyncRequiredState{a}, []types.SlidingSyncRequiredState{a, b}) {
		t.Error("expected configs to differ")
	}
	if !requiredStateConfigsEqual(nil, nil) {
		t.Error("expected empty configs to be equal")
	}
}
