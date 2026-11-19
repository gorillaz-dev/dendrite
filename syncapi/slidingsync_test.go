package syncapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/internal/httputil"
	"github.com/element-hq/dendrite/internal/sqlutil"
	rsapi "github.com/element-hq/dendrite/roomserver/api"
	rstypes "github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/setup/config"
	"github.com/element-hq/dendrite/setup/jetstream"
	"github.com/element-hq/dendrite/test"
	"github.com/element-hq/dendrite/test/testrig"
	userapi "github.com/element-hq/dendrite/userapi/api"
)

const slidingSyncPath = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"

// slidingSyncRoomserverAPI extends syncRoomserverAPI with the state queries used by sliding sync.
type slidingSyncRoomserverAPI struct {
	syncRoomserverAPI
}

// QueryStateAfterEvents returns the state of the test room after the given event.
func (s *slidingSyncRoomserverAPI) QueryStateAfterEvents(ctx context.Context, req *rsapi.QueryStateAfterEventsRequest, res *rsapi.QueryStateAfterEventsResponse) error {
	for _, room := range s.rooms {
		if room.ID != req.RoomID {
			continue
		}
		res.RoomExists = true
		state := map[gomatrixserverlib.StateKeyTuple]*rstypes.HeaderedEvent{}
		for _, ev := range room.Events() {
			if ev.StateKey() != nil {
				state[gomatrixserverlib.StateKeyTuple{EventType: ev.Type(), StateKey: *ev.StateKey()}] = ev
			}
			if len(req.PrevEventIDs) == 1 && ev.EventID() == req.PrevEventIDs[0] {
				res.PrevEventsExist = true
				break
			}
		}
		if !res.PrevEventsExist {
			return nil
		}
		for _, ev := range state {
			res.StateEvents = append(res.StateEvents, ev)
		}
	}
	return nil
}

// slidingSyncUserAPI extends syncUserAPI with account data.
type slidingSyncUserAPI struct {
	syncUserAPI
	globalAccountData map[string]json.RawMessage
	roomAccountData   map[string]map[string]json.RawMessage
}

func (s *slidingSyncUserAPI) QueryAccountData(ctx context.Context, req *userapi.QueryAccountDataRequest, res *userapi.QueryAccountDataResponse) error {
	res.GlobalAccountData = map[string]json.RawMessage{}
	res.RoomAccountData = map[string]map[string]json.RawMessage{}
	for dataType, data := range s.globalAccountData {
		if req.DataType == "" || req.DataType == dataType {
			res.GlobalAccountData[dataType] = data
		}
	}
	if req.DataType == "" {
		res.RoomAccountData = s.roomAccountData
	}
	return nil
}

type slidingSyncTest struct {
	t       *testing.T
	cfg     *config.Dendrite
	routers httputil.Routers
	jsctx   nats.JetStreamContext
}

func newSlidingSyncTest(t *testing.T, dbType test.DBType, enabled bool, userAPI *slidingSyncUserAPI, rooms ...*test.Room) *slidingSyncTest {
	t.Helper()
	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	if enabled {
		cfg.MSCs.MSCs = []string{"msc4186"}
	}
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	t.Cleanup(func() {
		jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
		closeRig()
	})
	rsAPI := &slidingSyncRoomserverAPI{syncRoomserverAPI{rooms: rooms}}
	AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, userAPI, rsAPI, caches, caching.DisableMetrics)
	return &slidingSyncTest{t: t, cfg: cfg, routers: routers, jsctx: jsctx}
}

// publish sends the events to the sync API, as if they came from the roomserver.
func (s *slidingSyncTest) publish(events ...*rstypes.HeaderedEvent) {
	s.t.Helper()
	testrig.MustPublishMsgs(s.t, s.jsctx, toNATSMsgs(s.t, s.cfg, events...)...)
}

// publishInvite sends an invite to the sync API, as if it came from the roomserver.
func (s *slidingSyncTest) publishInvite(invite *rstypes.HeaderedEvent) {
	s.t.Helper()
	testrig.MustPublishMsgs(s.t, s.jsctx, testrig.NewOutputEventMsg(s.t, s.cfg, invite.RoomID().String(), rsapi.OutputEvent{
		Type: rsapi.OutputTypeNewInviteEvent,
		NewInviteEvent: &rsapi.OutputNewInviteEvent{
			Event:       invite,
			RoomVersion: invite.Version(),
		},
	}))
}

// request sends a sliding sync request and returns the status code and body.
func (s *slidingSyncTest) request(accessToken string, body any) (int, gjson.Result) {
	s.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		s.t.Fatalf("failed to marshal body: %s", err)
	}
	req := httptest.NewRequest(http.MethodPost, slidingSyncPath, bytes.NewReader(data))
	req.Header.Set("Authorization", "Bearer "+accessToken)
	w := httptest.NewRecorder()
	s.routers.Client.ServeHTTP(w, req)
	return w.Code, gjson.ParseBytes(w.Body.Bytes())
}

// mustRequest sends a sliding sync request which must succeed.
func (s *slidingSyncTest) mustRequest(accessToken string, body any) gjson.Result {
	s.t.Helper()
	code, res := s.request(accessToken, body)
	if code != http.StatusOK {
		s.t.Fatalf("got HTTP %d, want 200: %s", code, res.Raw)
	}
	if !res.Get("pos").Exists() {
		s.t.Fatalf("response has no pos: %s", res.Raw)
	}
	return res
}

// waitFor sends requests on new connections until check returns true.
func (s *slidingSyncTest) waitFor(accessToken string, body map[string]any, check func(res gjson.Result) bool) {
	s.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if check(s.mustRequest(accessToken, body)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.t.Fatalf("timed out waiting for sliding sync response")
}

func newTestDevice(user *test.User, deviceID string) userapi.Device {
	return userapi.Device{
		ID:          deviceID,
		UserID:      user.ID,
		AccessToken: deviceID + "_TOKEN",
		AccountType: userapi.AccountTypeUser,
	}
}

func listRequest(conn string, rangeEnd int, timelineLimit int, requiredState map[string]any) map[string]any {
	return map[string]any{
		"conn_id": conn,
		"lists": map[string]any{
			"all": map[string]any{
				"range":          []int{0, rangeEnd},
				"timeline_limit": timelineLimit,
				"required_state": requiredState,
			},
		},
	}
}

func eventIDs(events gjson.Result) []string {
	var ids []string
	for _, ev := range events.Array() {
		ids = append(ids, ev.Get("event_id").Str)
	}
	return ids
}

func TestSlidingSyncDisabled(t *testing.T) {
	alice := test.NewUser(t)
	aliceDev := newTestDevice(alice, "ALICE")
	s := newSlidingSyncTest(t, test.DBTypeSQLite, false, &slidingSyncUserAPI{syncUserAPI: syncUserAPI{accounts: []userapi.Device{aliceDev}}})
	if code, _ := s.request(aliceDev.AccessToken, map[string]any{}); code != http.StatusNotFound {
		t.Fatalf("got HTTP %d, want 404", code)
	}
}

func TestSlidingSyncValidation(t *testing.T) {
	alice := test.NewUser(t)
	aliceDev := newTestDevice(alice, "ALICE")
	s := newSlidingSyncTest(t, test.DBTypeSQLite, true, &slidingSyncUserAPI{syncUserAPI: syncUserAPI{accounts: []userapi.Device{aliceDev}}})

	// Requests must be authenticated.
	if code, _ := s.request("unknown", map[string]any{}); code != http.StatusUnauthorized {
		t.Fatalf("got HTTP %d, want 401", code)
	}
	code, res := s.request(aliceDev.AccessToken, map[string]any{"set_presence": "invalid"})
	if code != http.StatusBadRequest || res.Get("errcode").Str != string(spec.ErrorInvalidParam) {
		t.Fatalf("got HTTP %d %s, want 400 M_INVALID_PARAM", code, res.Raw)
	}
	code, res = s.request(aliceDev.AccessToken, map[string]any{"pos": "garbage"})
	if code != http.StatusBadRequest || res.Get("errcode").Str != string(spec.ErrorUnknownPos) {
		t.Fatalf("got HTTP %d %s, want 400 M_UNKNOWN_POS", code, res.Raw)
	}
	// An initial request without any lists returns an empty response.
	res = s.mustRequest(aliceDev.AccessToken, map[string]any{})
	if len(res.Get("rooms").Map()) != 0 {
		t.Fatalf("expected no rooms: %s", res.Raw)
	}
}

func TestSlidingSync(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		testSlidingSync(t, dbType)
	})
}

// nolint:gocyclo
func testSlidingSync(t *testing.T, dbType test.DBType) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	aliceDev := newTestDevice(alice, "ALICE")
	aliceDev2 := newTestDevice(alice, "ALICE2")
	bobDev := newTestDevice(bob, "BOB")

	room1 := test.NewRoom(t, alice)
	room1.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "Room 1"}, test.WithStateKey(""))
	for i := 0; i < 5; i++ {
		room1.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": fmt.Sprintf("message %d", i)})
	}
	room2 := test.NewRoom(t, alice)
	room2Msg := room2.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hello"})
	bobRoom := test.NewRoom(t, bob)

	userAPI := &slidingSyncUserAPI{syncUserAPI: syncUserAPI{accounts: []userapi.Device{aliceDev, aliceDev2, bobDev}}}
	s := newSlidingSyncTest(t, dbType, true, userAPI, room1, room2, bobRoom)
	s.publish(room1.Events()...)
	s.publish(room2.Events()...)
	s.publish(bobRoom.Events()...)

	requiredState := map[string]any{
		"include": []map[string]any{
			{"type": spec.MRoomCreate, "state_key": ""},
			{"type": spec.MRoomName, "state_key": ""},
		},
	}
	s.waitFor(aliceDev.AccessToken, listRequest("", 1, 1, requiredState), func(res gjson.Result) bool {
		return res.Get("lists.all.count").Int() == 2 &&
			res.Get(fmt.Sprintf("rooms.%s.timeline.0.event_id", gjson.Escape(room2.ID))).Str == room2Msg.EventID()
	})

	// Initial request: only the most recently active room is in range.
	req := listRequest("main", 0, 1, requiredState)
	req["room_subscriptions"] = map[string]any{
		// Alice is not allowed to see this room, so the subscription must be ignored.
		bobRoom.ID: map[string]any{"timeline_limit": 10, "required_state": map[string]any{"include": []map[string]any{{}}}},
	}
	res := s.mustRequest(aliceDev.AccessToken, req)
	if got := res.Get("lists.all.count").Int(); got != 2 {
		t.Fatalf("got count %d, want 2: %s", got, res.Raw)
	}
	rooms := res.Get("rooms").Map()
	if len(rooms) != 1 {
		t.Fatalf("got %d rooms, want 1: %s", len(rooms), res.Raw)
	}
	r2 := rooms[room2.ID]
	if !r2.Get("initial").Bool() || r2.Get("membership").Str != spec.Join {
		t.Fatalf("expected initial joined room: %s", r2.Raw)
	}
	if ids := eventIDs(r2.Get("timeline")); len(ids) != 1 || ids[0] != room2Msg.EventID() {
		t.Fatalf("unexpected timeline %v: %s", ids, r2.Raw)
	}
	if !r2.Get("limited").Bool() || r2.Get("prev_batch").Str == "" {
		t.Fatalf("expected limited timeline with prev_batch: %s", r2.Raw)
	}
	if r2.Get("lists.0").Str != "all" || !r2.Get("bump_stamp").Exists() || r2.Get("joined_count").Int() != 1 {
		t.Fatalf("missing room fields: %s", r2.Raw)
	}
	if state := r2.Get("required_state").Array(); len(state) != 1 || state[0].Get("type").Str != spec.MRoomCreate {
		t.Fatalf("expected only the create event in required_state: %s", r2.Raw)
	}
	if r2.Get("name").Exists() {
		t.Fatalf("room 2 has no name: %s", r2.Raw)
	}
	pos := res.Get("pos").Str

	// The position cannot be used by another device or user.
	for _, token := range []string{aliceDev2.AccessToken, bobDev.AccessToken} {
		code, errRes := s.request(token, map[string]any{"conn_id": "main", "pos": pos})
		if code != http.StatusBadRequest || errRes.Get("errcode").Str != string(spec.ErrorUnknownPos) {
			t.Fatalf("got HTTP %d %s, want 400 M_UNKNOWN_POS", code, errRes.Raw)
		}
	}

	// The position can be used with /messages.
	w := httptest.NewRecorder()
	s.routers.Client.ServeHTTP(w, test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/rooms/"+room2.ID+"/messages", test.WithQueryParams(map[string]string{
		"access_token": aliceDev.AccessToken, "dir": "b", "from": pos,
	})))
	if w.Code != http.StatusOK {
		t.Fatalf("/messages with pos: got HTTP %d: %s", w.Code, w.Body.String())
	}

	// Expanding the range returns the rooms which were not sent yet.
	req = listRequest("main", 1, 1, requiredState)
	req["pos"] = pos
	res = s.mustRequest(aliceDev.AccessToken, req)
	rooms = res.Get("rooms").Map()
	if len(rooms) != 1 {
		t.Fatalf("got %d rooms, want 1: %s", len(rooms), res.Raw)
	}
	r1 := rooms[room1.ID]
	if !r1.Get("initial").Bool() || r1.Get("name").Str != "Room 1" {
		t.Fatalf("expected initial room 1 with name: %s", r1.Raw)
	}
	if ids := eventIDs(r1.Get("timeline")); len(ids) != 1 || ids[0] != room1.Events()[len(room1.Events())-1].EventID() {
		t.Fatalf("unexpected timeline %v", ids)
	}
	if r1.Get("num_live").Int() != 0 {
		t.Fatalf("historical events must not be live: %s", r1.Raw)
	}
	pos = res.Get("pos").Str

	// Retrying the request with the same position returns the same data again.
	retry := s.mustRequest(aliceDev.AccessToken, req)
	if _, ok := retry.Get("rooms").Map()[room1.ID]; !ok {
		t.Fatalf("expected retried request to return room 1 again: %s", retry.Raw)
	}

	// Increasing the timeline limit returns older events immediately.
	req = listRequest("main", 1, 3, requiredState)
	req["pos"] = pos
	req["timeout"] = 10000
	start := time.Now()
	res = s.mustRequest(aliceDev.AccessToken, req)
	if time.Since(start) > 5*time.Second {
		t.Fatalf("expanded timelines should be returned immediately")
	}
	r1 = res.Get("rooms").Map()[room1.ID]
	if !r1.Get("expanded_timeline").Bool() || len(r1.Get("timeline").Array()) != 3 || r1.Get("initial").Bool() {
		t.Fatalf("expected expanded timeline for room 1: %s", res.Raw)
	}
	if r1.Get("name").Exists() || r1.Get("required_state").Exists() {
		t.Fatalf("unchanged fields must not be sent again: %s", r1.Raw)
	}
	r2 = res.Get("rooms").Map()[room2.ID]
	if !r2.Get("expanded_timeline").Bool() {
		t.Fatalf("expected expanded timeline for room 2: %s", res.Raw)
	}
	pos = res.Get("pos").Str

	// Nothing changed, so a request with a short timeout returns no rooms.
	req["pos"] = pos
	req["timeout"] = 100
	res = s.mustRequest(aliceDev.AccessToken, req)
	if len(res.Get("rooms").Map()) != 0 {
		t.Fatalf("expected no rooms: %s", res.Raw)
	}
	pos = res.Get("pos").Str

	// New events wake up the request and only the changed room is returned.
	type result struct {
		res gjson.Result
	}
	done := make(chan result)
	req["pos"] = pos
	req["timeout"] = 10000
	go func() {
		_, r := s.request(aliceDev.AccessToken, req)
		done <- result{r}
	}()
	time.Sleep(200 * time.Millisecond)
	newMsg := room1.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "new"})
	newName := room1.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "Renamed"}, test.WithStateKey(""))
	s.publish(newMsg, newName)
	var got result
	select {
	case got = <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for long-poll")
	}
	res = got.res
	rooms = res.Get("rooms").Map()
	r1, ok := rooms[room1.ID]
	if !ok || len(rooms) != 1 {
		// The two events may arrive in separate responses.
		t.Logf("first response: %s", res.Raw)
	}
	if !ok {
		t.Fatalf("expected room 1 in response: %s", res.Raw)
	}
	if r1.Get("initial").Bool() || r1.Get("expanded_timeline").Bool() {
		t.Fatalf("expected an incremental update: %s", r1.Raw)
	}
	ids := eventIDs(r1.Get("timeline"))
	if len(ids) == 0 || ids[0] != newMsg.EventID() {
		t.Fatalf("expected new message in timeline, got %v: %s", ids, r1.Raw)
	}
	if r1.Get("num_live").Int() != int64(len(ids)) || r1.Get("limited").Bool() {
		t.Fatalf("expected live, non-limited timeline: %s", r1.Raw)
	}
	if r1.Get("membership").Exists() {
		t.Fatalf("membership did not change: %s", r1.Raw)
	}
	// Wait for the rename, which may arrive in a separate response.
	for r1.Get("name").Str != "Renamed" {
		if len(ids) == 2 {
			t.Fatalf("expected new name: %s", r1.Raw)
		}
		req["pos"] = res.Get("pos").Str
		res = s.mustRequest(aliceDev.AccessToken, req)
		r1 = res.Get("rooms").Map()[room1.ID]
		ids = eventIDs(r1.Get("timeline"))
		if len(ids) > 0 && ids[len(ids)-1] != newName.EventID() {
			t.Fatalf("unexpected timeline %v", ids)
		}
	}
	if state := r1.Get("required_state").Array(); len(state) != 1 || state[0].Get("content.name").Str != "Renamed" {
		t.Fatalf("expected the changed name in required_state: %s", r1.Raw)
	}
}

func TestSlidingSyncMemberships(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		testSlidingSyncMemberships(t, dbType)
	})
}

// nolint:gocyclo
func testSlidingSyncMemberships(t *testing.T, dbType test.DBType) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	aliceDev := newTestDevice(alice, "ALICE")
	bobDev := newTestDevice(bob, "BOB")

	// A room Alice joins and later leaves.
	roomA := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
	roomA.CreateAndInsert(t, bob, spec.MRoomName, map[string]any{"name": "Room A"}, test.WithStateKey(""))
	aliceJoin := roomA.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(alice.ID))
	bobMsg := roomA.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "hi alice"})

	// A room Alice is invited to.
	inviteRoom := test.NewRoom(t, bob)
	inviteRoomName := inviteRoom.CreateAndInsert(t, bob, spec.MRoomName, map[string]any{"name": "Invite room"}, test.WithStateKey(""))
	invite := inviteRoom.CreateEvent(t, bob, spec.MRoomMember, map[string]any{"membership": "invite"},
		test.WithStateKey(alice.ID),
		test.WithUnsigned(map[string]any{"invite_room_state": []map[string]any{{
			"type": spec.MRoomName, "state_key": "", "sender": bob.ID, "content": map[string]any{"name": "Invite room"},
		}}}),
	)

	// A DM room.
	dmRoom := test.NewRoom(t, alice)

	userAPI := &slidingSyncUserAPI{
		syncUserAPI:       syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}},
		globalAccountData: map[string]json.RawMessage{"m.direct": json.RawMessage(fmt.Sprintf(`{%q: [%q]}`, bob.ID, dmRoom.ID))},
	}
	s := newSlidingSyncTest(t, dbType, true, userAPI, roomA, inviteRoom, dmRoom)
	s.publish(roomA.Events()...)
	s.publish(dmRoom.Events()...)
	s.publish(inviteRoom.Events()...)
	s.publishInvite(invite)

	roomConfig := func(filters map[string]any) map[string]any {
		return map[string]any{
			"range":          []int{0, 10},
			"timeline_limit": 5,
			"required_state": map[string]any{"include": []map[string]any{{}}, "lazy_members": true},
			"filters":        filters,
		}
	}
	req := map[string]any{
		"lists": map[string]any{
			"all":     roomConfig(nil),
			"invites": roomConfig(map[string]any{"is_invited": true}),
			"dms":     roomConfig(map[string]any{"is_dm": true}),
			"spaces":  roomConfig(map[string]any{"room_types": []string{"m.space"}}),
		},
	}
	var res gjson.Result
	s.waitFor(aliceDev.AccessToken, req, func(r gjson.Result) bool {
		res = r
		return r.Get("lists.all.count").Int() == 3 &&
			r.Get(fmt.Sprintf("rooms.%s.timeline.#(event_id==%q)", gjson.Escape(roomA.ID), bobMsg.EventID())).Exists()
	})
	if got := res.Get("lists.invites.count").Int(); got != 1 {
		t.Fatalf("got %d invites, want 1: %s", got, res.Raw)
	}
	if got := res.Get("lists.dms.count").Int(); got != 1 {
		t.Fatalf("got %d DMs, want 1: %s", got, res.Raw)
	}
	if got := res.Get("lists.spaces.count").Int(); got != 0 {
		t.Fatalf("got %d spaces, want 0: %s", got, res.Raw)
	}

	// Invites only contain stripped state.
	inv := res.Get("rooms").Map()[inviteRoom.ID]
	if inv.Get("membership").Str != spec.Invite {
		t.Fatalf("expected invite membership: %s", inv.Raw)
	}
	if inv.Get("timeline").Exists() || inv.Get("required_state").Exists() || inv.Get("name").Exists() {
		t.Fatalf("invites must only contain stripped state: %s", inv.Raw)
	}
	if !inv.Get(`stripped_state.#(type=="m.room.name").content.name`).Exists() ||
		!inv.Get(`stripped_state.#(type=="m.room.member").state_key`).Exists() {
		t.Fatalf("expected stripped state with name and invite: %s", inv.Raw)
	}
	if inv.Get(fmt.Sprintf(`stripped_state.#(event_id==%q)`, inviteRoomName.EventID())).Exists() {
		t.Fatalf("stripped state must not contain full events: %s", inv.Raw)
	}

	dm := res.Get("rooms").Map()[dmRoom.ID]
	if !dm.Get("is_dm").Bool() {
		t.Fatalf("expected DM room to be flagged: %s", dm.Raw)
	}
	if lists := dm.Get("lists").Array(); len(lists) != 2 {
		t.Fatalf("expected DM room to be in two lists: %s", dm.Raw)
	}

	// Lazy loading: the memberships of the timeline senders are included, as is all other state.
	ra := res.Get("rooms").Map()[roomA.ID]
	if !ra.Get(fmt.Sprintf(`required_state.#(state_key==%q)`, bob.ID)).Exists() ||
		!ra.Get(fmt.Sprintf(`required_state.#(state_key==%q)`, alice.ID)).Exists() ||
		!ra.Get(`required_state.#(type=="m.room.join_rules")`).Exists() {
		t.Fatalf("expected lazy loaded members and state: %s", ra.Raw)
	}
	if ra.Get("joined_count").Int() != 2 {
		t.Fatalf("expected 2 joined members: %s", ra.Raw)
	}
	pos := res.Get("pos").Str

	// Alice leaves the room, afterwards Bob sends a message and renames the room.
	aliceLeave := roomA.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": "leave"}, test.WithStateKey(alice.ID))
	secret := roomA.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "secret"})
	rename := roomA.CreateAndInsert(t, bob, spec.MRoomName, map[string]any{"name": "Secret name"}, test.WithStateKey(""))
	s.publish(aliceLeave, secret, rename)

	// Wait until the leave arrives. Make sure nothing that happened afterwards is revealed.
	deadline := time.Now().Add(10 * time.Second)
	sawLeave := false
	for !sawLeave && time.Now().Before(deadline) {
		req["pos"] = pos
		res = s.mustRequest(aliceDev.AccessToken, req)
		pos = res.Get("pos").Str
		if strings.Contains(res.Raw, secret.EventID()) || strings.Contains(res.Raw, rename.EventID()) || strings.Contains(res.Raw, "Secret name") {
			t.Fatalf("events after leaving the room were revealed: %s", res.Raw)
		}
		left, ok := res.Get("rooms").Map()[roomA.ID]
		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if left.Get("membership").Str == spec.Leave {
			sawLeave = true
			if !left.Get(fmt.Sprintf(`timeline.#(event_id==%q)`, aliceLeave.EventID())).Exists() {
				t.Fatalf("expected leave event in timeline: %s", left.Raw)
			}
		}
	}
	if !sawLeave {
		t.Fatalf("timed out waiting for leave")
	}
	// Wait until the later events have been processed, and check they still don't show up.
	s.waitFor(bobDev.AccessToken, map[string]any{"room_subscriptions": map[string]any{
		roomA.ID: map[string]any{"timeline_limit": 1, "required_state": map[string]any{}},
	}}, func(r gjson.Result) bool {
		return r.Get(fmt.Sprintf("rooms.%s.timeline.0.event_id", gjson.Escape(roomA.ID))).Str == rename.EventID()
	})
	req["pos"] = pos
	res = s.mustRequest(aliceDev.AccessToken, req)
	if strings.Contains(res.Raw, secret.EventID()) || strings.Contains(res.Raw, "Secret name") {
		t.Fatalf("events after leaving the room were revealed: %s", res.Raw)
	}
	// A room subscription doesn't reveal them either.
	sub := map[string]any{"room_subscriptions": map[string]any{
		roomA.ID: map[string]any{"timeline_limit": 10, "required_state": map[string]any{"include": []map[string]any{{}}}},
	}}
	res = s.mustRequest(aliceDev.AccessToken, sub)
	ra = res.Get("rooms").Map()[roomA.ID]
	if ra.Get("membership").Str != spec.Leave || ra.Get("name").Str != "Room A" {
		t.Fatalf("expected left room with the name before leaving: %s", res.Raw)
	}
	if strings.Contains(res.Raw, secret.EventID()) || strings.Contains(res.Raw, "Secret name") {
		t.Fatalf("events after leaving the room were revealed: %s", res.Raw)
	}

	// Rooms the user left themselves are not part of the list on new connections.
	newConn := s.mustRequest(aliceDev.AccessToken, map[string]any{"lists": map[string]any{"all": roomConfig(nil)}})
	if got := newConn.Get("lists.all.count").Int(); got != 2 {
		t.Fatalf("got count %d, want 2: %s", got, newConn.Raw)
	}
	if _, ok := newConn.Get("rooms").Map()[roomA.ID]; ok {
		t.Fatalf("left room must not be returned on a new connection: %s", newConn.Raw)
	}
	_ = aliceJoin
}

func TestSlidingSyncKickedAndBanned(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		aliceDev := newTestDevice(alice, "ALICE")
		bobDev := newTestDevice(bob, "BOB")

		// Alice is kicked from a room she was joined to.
		kickRoom := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
		kickRoom.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(alice.ID))
		kick := kickRoom.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "leave"}, test.WithStateKey(alice.ID))
		secret := kickRoom.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "secret"})

		// Alice is banned from a room she was never joined to.
		banRoom := test.NewRoom(t, bob)
		banRoom.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "ban"}, test.WithStateKey(alice.ID))
		banMsg := banRoom.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "hidden"})

		userAPI := &slidingSyncUserAPI{syncUserAPI: syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}}}
		s := newSlidingSyncTest(t, dbType, true, userAPI, kickRoom, banRoom)
		s.publish(kickRoom.Events()...)
		s.publish(banRoom.Events()...)

		// Wait until all events have been processed.
		s.waitFor(bobDev.AccessToken, listRequest("", 10, 1, map[string]any{}), func(r gjson.Result) bool {
			return r.Get(fmt.Sprintf("rooms.%s.timeline.0.event_id", gjson.Escape(kickRoom.ID))).Str == secret.EventID() &&
				r.Get(fmt.Sprintf("rooms.%s.timeline.0.event_id", gjson.Escape(banRoom.ID))).Str == banMsg.EventID()
		})

		req := listRequest("", 10, 10, map[string]any{"include": []map[string]any{{}}})
		req["room_subscriptions"] = map[string]any{
			banRoom.ID: map[string]any{"timeline_limit": 10, "required_state": map[string]any{"include": []map[string]any{{}}}},
		}
		res := s.mustRequest(aliceDev.AccessToken, req)
		if got := res.Get("lists.all.count").Int(); got != 1 {
			t.Fatalf("got count %d, want 1: %s", got, res.Raw)
		}
		if _, ok := res.Get("rooms").Map()[banRoom.ID]; ok {
			t.Fatalf("rooms the user was banned from without joining must not be returned: %s", res.Raw)
		}
		kicked := res.Get("rooms").Map()[kickRoom.ID]
		if kicked.Get("membership").Str != spec.Leave || !kicked.Get("initial").Bool() {
			t.Fatalf("expected kicked room: %s", res.Raw)
		}
		ids := eventIDs(kicked.Get("timeline"))
		if len(ids) == 0 || ids[len(ids)-1] != kick.EventID() {
			t.Fatalf("expected the kick to be the last timeline event, got %v", ids)
		}
		if strings.Contains(res.Raw, secret.EventID()) || strings.Contains(res.Raw, banMsg.EventID()) {
			t.Fatalf("events the user may not see were revealed: %s", res.Raw)
		}
		if !kicked.Get(fmt.Sprintf(`required_state.#(state_key==%q).content.membership`, alice.ID)).Exists() {
			t.Fatalf("expected alice's membership in required_state: %s", kicked.Raw)
		}
	})
}
