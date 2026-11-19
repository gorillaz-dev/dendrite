package storage_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/tidwall/gjson"

	rstypes "github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/types"
	"github.com/element-hq/dendrite/test"
)

// nolint:gocyclo
func TestSlidingSyncStorage(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		carol := test.NewUser(t)

		// Alice creates room 1 and invites Carol.
		room1 := test.NewRoom(t, alice)
		r1Msg := room1.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hello"})
		room1.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": "invite"}, test.WithStateKey(carol.ID))
		r1Topic := room1.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "topic"}, test.WithStateKey(""))

		// Alice joins room 2 and is kicked by Bob.
		room2 := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
		room2.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(alice.ID))
		r2Msg := room2.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "bye"})
		r2Kick := room2.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "leave"}, test.WithStateKey(alice.ID))

		// The sync API consumer resolves the user ID of the sender before writing events.
		for _, ev := range append(room1.Events(), room2.Events()...) {
			userID, err := spec.NewUserID(string(ev.SenderID()), true)
			if err != nil {
				t.Fatal(err)
			}
			ev.UserID = *userID
		}

		db, closeDB := MustCreateDatabase(t, dbType)
		defer closeDB()
		r1Positions := MustWriteEvents(t, db, room1.Events())
		r2Positions := MustWriteEvents(t, db, room2.Events())
		r1Last := r1Positions[len(r1Positions)-1]
		r2Last := r2Positions[len(r2Positions)-1]
		posOf := func(room *test.Room, positions []types.StreamPosition, ev *rstypes.HeaderedEvent) types.StreamPosition {
			for i, e := range room.Events() {
				if e.EventID() == ev.EventID() {
					return positions[i]
				}
			}
			t.Fatalf("event not found")
			return 0
		}

		withRollbackSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			// MaxStreamPositionsForRooms
			positions, err := snapshot.MaxStreamPositionsForRooms(ctx, []string{room1.ID, room2.ID, "!unknown:test"}, nil, r2Last)
			if err != nil {
				t.Fatal(err)
			}
			if len(positions) != 2 || positions[room1.ID] != r1Last || positions[room2.ID] != r2Last {
				t.Fatalf("unexpected latest positions: %v", positions)
			}
			positions, err = snapshot.MaxStreamPositionsForRooms(ctx, []string{room1.ID, room2.ID}, []string{"m.room.message", "m.sticker"}, r2Last)
			if err != nil {
				t.Fatal(err)
			}
			if positions[room1.ID] != posOf(room1, r1Positions, r1Msg) || positions[room2.ID] != posOf(room2, r2Positions, r2Msg) {
				t.Fatalf("unexpected message positions: %v", positions)
			}
			// Events after maxPos are ignored.
			positions, err = snapshot.MaxStreamPositionsForRooms(ctx, []string{room1.ID, room2.ID}, nil, r1Last)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := positions[room2.ID]; ok || positions[room1.ID] != r1Last {
				t.Fatalf("unexpected positions with maxPos: %v", positions)
			}
			positions, err = snapshot.MaxStreamPositionsForRooms(ctx, nil, nil, r2Last)
			if err != nil || len(positions) != 0 {
				t.Fatalf("expected no positions, got %v (err %v)", positions, err)
			}

			// MembershipEventsForUser
			memberships, err := snapshot.MembershipEventsForUser(ctx, alice.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(memberships) != 2 {
				t.Fatalf("got %d memberships, want 2: %v", len(memberships), memberships)
			}
			if m := memberships[room1.ID]; m.Membership != spec.Join || m.Sender != alice.ID || m.EventID == "" || m.StreamPos == 0 {
				t.Fatalf("unexpected membership in room 1: %+v", m)
			}
			want := types.RoomMembership{Membership: spec.Leave, EventID: r2Kick.EventID(), Sender: bob.ID, StreamPos: r2Last}
			if m := memberships[room2.ID]; m != want {
				t.Fatalf("got membership %+v, want %+v", m, want)
			}
			memberships, err = snapshot.MembershipEventsForUser(ctx, carol.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(memberships) != 1 || memberships[room1.ID].Membership != spec.Invite {
				t.Fatalf("unexpected memberships for carol: %v", memberships)
			}

			// RoomsEverJoined
			everJoined, err := snapshot.RoomsEverJoined(ctx, alice.ID, []string{room1.ID, room2.ID, "!unknown:test"})
			if err != nil {
				t.Fatal(err)
			}
			if len(everJoined) != 2 {
				t.Fatalf("expected alice to have joined both rooms: %v", everJoined)
			}
			everJoined, err = snapshot.RoomsEverJoined(ctx, carol.ID, []string{room1.ID, room2.ID})
			if err != nil {
				t.Fatal(err)
			}
			if len(everJoined) != 0 {
				t.Fatalf("expected carol to have joined no rooms: %v", everJoined)
			}

			// StateTuplesChangedInRange
			from := posOf(room1, r1Positions, r1Msg)
			changed, err := snapshot.StateTuplesChangedInRange(ctx, []string{room1.ID, room2.ID}, types.Range{From: from, To: r1Last})
			if err != nil {
				t.Fatal(err)
			}
			if len(changed) != 1 {
				t.Fatalf("expected only room 1 to have changes: %v", changed)
			}
			assertTuples(t, changed[room1.ID], []gomatrixserverlib.StateKeyTuple{
				{EventType: spec.MRoomMember, StateKey: carol.ID},
				{EventType: "m.room.topic", StateKey: ""},
			})
		})

		// State which is removed without being replaced is reported as well.
		removal := room1.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "state reset"})
		removalPos, err := db.WriteEvent(ctx, removal, nil, nil, []string{r1Topic.EventID()}, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		if err != nil {
			t.Fatal(err)
		}
		withRollbackSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			changed, err := snapshot.StateTuplesChangedInRange(ctx, []string{room1.ID}, types.Range{From: r2Last, To: removalPos})
			if err != nil {
				t.Fatal(err)
			}
			assertTuples(t, changed[room1.ID], []gomatrixserverlib.StateKeyTuple{{EventType: "m.room.topic", StateKey: ""}})
		})
	})
}

func assertTuples(t *testing.T, got, want []gomatrixserverlib.StateKeyTuple) {
	t.Helper()
	sortTuples := func(tuples []gomatrixserverlib.StateKeyTuple) {
		sort.Slice(tuples, func(i, j int) bool {
			if tuples[i].EventType != tuples[j].EventType {
				return tuples[i].EventType < tuples[j].EventType
			}
			return tuples[i].StateKey < tuples[j].StateKey
		})
	}
	sortTuples(got)
	sortTuples(want)
	if len(got) != len(want) {
		t.Fatalf("got tuples %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got tuples %v, want %v", got, want)
		}
	}
}

// withRollbackSnapshot is like WithSnapshot, but also rolls back if the test fails,
// as an open transaction would block the cleanup of the database.
func withRollbackSnapshot(t *testing.T, db storage.Database, f func(snapshot storage.DatabaseTransaction)) {
	t.Helper()
	snapshot, err := db.NewDatabaseSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Rollback() // nolint:errcheck
	f(snapshot)
}

func TestAllocatedSendToDevicePosition(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := MustCreateDatabase(t, dbType)
		defer closeDB()

		withRollbackSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			pos, err := snapshot.AllocatedSendToDevicePosition(ctx)
			if err != nil || pos != 0 {
				t.Fatalf("got position %d (err %v), want 0", pos, err)
			}
		})

		var last types.StreamPosition
		for i := 0; i < 2; i++ {
			var err error
			last, err = db.StoreNewSendForDeviceMessage(ctx, "@alice:test", "ALICE", gomatrixserverlib.SendToDeviceEvent{
				Sender: "@alice:test", Type: "m.test", Content: []byte(`{}`),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		// Deleting the messages doesn't decrease the allocated position.
		if err := db.CleanSendToDeviceUpdates(ctx, "@alice:test", "ALICE", last); err != nil {
			t.Fatal(err)
		}
		withRollbackSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			maxPos, err := snapshot.MaxStreamPositionForSendToDeviceMessages(ctx)
			if err != nil {
				t.Fatal(err)
			}
			allocated, err := snapshot.AllocatedSendToDevicePosition(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if allocated != last || maxPos >= last {
				t.Fatalf("got allocated position %d and max position %d, want %d and less", allocated, maxPos, last)
			}
		})
	})
}

func TestSendToDeviceMessagesAfter(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := MustCreateDatabase(t, dbType)
		defer closeDB()

		var positions []types.StreamPosition
		for i := 0; i < 5; i++ {
			for _, deviceID := range []string{"ALICE", "OTHER"} {
				pos, err := db.StoreNewSendForDeviceMessage(ctx, "@alice:test", deviceID, gomatrixserverlib.SendToDeviceEvent{
					Sender: "@alice:test", Type: "m.test", Content: []byte(fmt.Sprintf(`{"n": %d}`, i)),
				})
				if err != nil {
					t.Fatal(err)
				}
				if deviceID == "ALICE" {
					positions = append(positions, pos)
				}
			}
		}

		withRollbackSnapshot(t, db, func(snapshot storage.DatabaseTransaction) {
			// The oldest messages of the device are returned, up to the limit.
			lastPos, events, err := snapshot.SendToDeviceMessagesAfter(ctx, "@alice:test", "ALICE", 0, 2)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 2 || events[0].ID != positions[0] || events[1].ID != positions[1] || lastPos != positions[1] {
				t.Fatalf("unexpected first batch: lastPos=%d events=%+v", lastPos, events)
			}
			if events[0].DeviceID != "ALICE" || gjson.GetBytes(events[0].Content, "n").Int() != 0 || !gjson.GetBytes(events[0].Content, "n").Exists() || events[0].Type != "m.test" {
				t.Fatalf("unexpected event: %+v", events[0])
			}
			// Continuing from the last position returns the following messages.
			lastPos, events, err = snapshot.SendToDeviceMessagesAfter(ctx, "@alice:test", "ALICE", lastPos, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 3 || events[0].ID != positions[2] || lastPos != positions[4] {
				t.Fatalf("unexpected second batch: lastPos=%d events=%+v", lastPos, events)
			}
			// Without further messages, the position doesn't change.
			lastPos, events, err = snapshot.SendToDeviceMessagesAfter(ctx, "@alice:test", "ALICE", positions[4], 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != 0 || lastPos != positions[4] {
				t.Fatalf("expected no messages: lastPos=%d events=%+v", lastPos, events)
			}
			// Messages of other devices are never returned.
			_, events, err = snapshot.SendToDeviceMessagesAfter(ctx, "@alice:test", "UNKNOWN", 0, 10)
			if err != nil || len(events) != 0 {
				t.Fatalf("expected no messages for unknown device: %+v (err %v)", events, err)
			}
		})
	})
}
