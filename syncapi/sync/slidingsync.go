// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"github.com/element-hq/dendrite/internal/sqlutil"
	"github.com/element-hq/dendrite/syncapi/types"
	userapi "github.com/element-hq/dendrite/userapi/api"
)

// slidingSyncMaxBodySize is the maximum size of a sliding sync request body.
const slidingSyncMaxBodySize = 1 << 20

// OnIncomingSlidingSyncRequest implements simplified sliding sync (MSC4186):
//
//	POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync
//
// This function MUST be called in a dedicated goroutine for this request, as it
// blocks until there is new data for the client or the timeout is reached.
func (rp *RequestPool) OnIncomingSlidingSyncRequest(req *http.Request, device *userapi.Device) util.JSONResponse {
	body, err := io.ReadAll(io.LimitReader(req.Body, slidingSyncMaxBodySize+1))
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be read."),
		}
	}
	if len(body) > slidingSyncMaxBodySize {
		return util.JSONResponse{
			Code: http.StatusRequestEntityTooLarge,
			JSON: spec.MatrixError{ErrCode: "M_TOO_LARGE", Err: "The request body is too large."},
		}
	}
	var ssReq types.SlidingSyncRequest
	if len(body) > 0 {
		if err = json.Unmarshal(body, &ssReq); err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.BadJSON("The request body could not be decoded into valid JSON."),
			}
		}
	}
	if err = ssReq.Validate(); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(err.Error()),
		}
	}

	// Connections are scoped to the user and device, so that positions issued to
	// another user or device are rejected.
	var conn *slidingSyncConnection
	var base *slidingSyncSnapshot
	if ssReq.Pos == "" {
		conn, base = rp.slidingSync.create(device.UserID, device.ID, ssReq.ConnID)
	} else {
		conn, base, err = rp.slidingSync.get(device.UserID, device.ID, ssReq.ConnID, ssReq.Pos)
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MatrixError{ErrCode: spec.ErrorUnknownPos, Err: "Unknown position, please start a new connection"},
			}
		}
	}

	activeSyncRequests.Inc()
	defer activeSyncRequests.Dec()

	rp.updateLastSeen(req, device)
	rp.updatePresence(rp.db, ssReq.SetPresence, device.UserID)

	ctx := req.Context()
	logger := util.GetLogger(ctx).WithField("conn_id", ssReq.ConnID)

	// The timeout is ignored for the first request of a connection.
	timeout := time.Duration(ssReq.Timeout) * time.Millisecond
	if ssReq.Pos == "" {
		timeout = 0
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	since := base.token
	for {
		currentPos := rp.Notifier.CurrentPosition()
		res, rooms, listCounts, changed, err := rp.slidingSyncResponse(ctx, conn, device, base, &ssReq, &currentPos)
		if err != nil {
			if ctx.Err() != nil {
				return util.JSONResponse{Code: http.StatusOK, JSON: types.NewSlidingSyncResponse()}
			}
			logger.WithError(err).Error("Failed to calculate sliding sync response")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		if changed || timeout <= 0 || !rp.waitForSlidingSyncUpdates(ctx, timer, device, &since, currentPos) {
			if ctx.Err() != nil {
				// The client gave up, so the response will never be processed.
				return util.JSONResponse{Code: http.StatusOK, JSON: types.NewSlidingSyncResponse()}
			}
			res.Pos = conn.addSnapshot(currentPos, rooms, listCounts).String()
			return util.JSONResponse{Code: http.StatusOK, JSON: res}
		}
	}
}

// waitForSlidingSyncUpdates waits until there may be new data relevant to sliding sync.
// Returns false if the timeout was reached or the client gave up.
func (rp *RequestPool) waitForSlidingSyncUpdates(
	ctx context.Context, timer *time.Timer, device *userapi.Device, since *types.StreamingToken, currentPos types.StreamingToken,
) bool {
	since.ApplyUpdates(currentPos)
	listener := rp.Notifier.GetListener(types.SyncRequest{Context: ctx, Device: device})
	defer listener.Close()
	waitingSyncRequests.Inc()
	defer waitingSyncRequests.Dec()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-listener.GetNotifyChannel(*since):
			newPos := listener.GetSyncPosition()
			// Only room events, invites and account data (for DM and tag filters) affect the
			// response. Ignore updates of other streams, e.g. typing notifications.
			relevant := newPos.PDUPosition > since.PDUPosition ||
				newPos.InvitePosition > since.InvitePosition ||
				newPos.AccountDataPosition > since.AccountDataPosition
			since.ApplyUpdates(newPos)
			if relevant {
				return true
			}
		}
	}
}

// slidingSyncResponse calculates the response for the given request, relative to the base
// snapshot. It returns the response, the rooms and list counts for the next snapshot, and
// whether there is anything new to return to the client.
// nolint:gocyclo
func (rp *RequestPool) slidingSyncResponse(
	ctx context.Context, conn *slidingSyncConnection, device *userapi.Device, base *slidingSyncSnapshot,
	ssReq *types.SlidingSyncRequest, currentPos *types.StreamingToken,
) (res *types.SlidingSyncResponse, rooms map[string]*slidingSyncRoomState, listCounts map[string]int, changed bool, err error) {
	snapshot, err := rp.db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("NewDatabaseSnapshot: %w", err)
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	// The database may already contain events the notifier hasn't seen yet. Use the position
	// of the snapshot, so that the timeline and the current state are consistent.
	maxPDUPos, err := snapshot.MaxStreamPositionForPDUs(ctx)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("snapshot.MaxStreamPositionForPDUs: %w", err)
	}
	currentPos.PDUPosition = max(currentPos.PDUPosition, maxPDUPos)

	ignored := &types.IgnoredUsers{}
	if ignores, ignoreErr := snapshot.IgnoresForUser(ctx, device.UserID); ignoreErr == nil {
		ignored = ignores
	} else if ignoreErr != sql.ErrNoRows {
		return nil, nil, nil, false, fmt.Errorf("snapshot.IgnoresForUser: %w", ignoreErr)
	}
	ignoredSenders := make([]string, 0, len(ignored.List))
	for userID := range ignored.List {
		ignoredSenders = append(ignoredSenders, userID)
	}

	roomList, err := rp.buildRoomList(ctx, snapshot, conn, device, base, currentPos.PDUPosition, ignored)
	if err != nil {
		return nil, nil, nil, false, err
	}
	filters := &slidingSyncFilterContext{
		ctx: ctx, rp: rp, snapshot: snapshot, conn: conn, device: device, list: roomList,
	}

	// Work out which rooms match the lists and subscriptions, and combine their configs.
	res = types.NewSlidingSyncResponse()
	listCounts = make(map[string]int, len(ssReq.Lists))
	configs := make(map[string]*slidingSyncRoomConfig)
	configFor := func(roomID string) *slidingSyncRoomConfig {
		cfg, ok := configs[roomID]
		if !ok {
			cfg = &slidingSyncRoomConfig{}
			configs[roomID] = cfg
		}
		return cfg
	}
	listNames := make([]string, 0, len(ssReq.Lists))
	for name := range ssReq.Lists {
		listNames = append(listNames, name)
	}
	sort.Strings(listNames)
	for _, name := range listNames {
		list := ssReq.Lists[name]
		count := 0
		for _, entry := range roomList.sorted {
			matches, matchErr := filters.matches(entry, list.Filters)
			if matchErr != nil {
				return nil, nil, nil, false, matchErr
			}
			if !matches {
				continue
			}
			index := int64(count)
			count++
			if list.Range != nil && (index < list.Range[0] || index > list.Range[1]) {
				continue
			}
			cfg := configFor(entry.roomID)
			cfg.add(*list.TimelineLimit, *list.RequiredState)
			cfg.lists = append(cfg.lists, name)
		}
		listCounts[name] = count
		res.Lists[name] = types.SlidingSyncListResult{Count: count}
		if prevCount, ok := base.listCounts[name]; !ok || prevCount != count {
			changed = true
		}
	}
	subscriptions := make([]string, 0, len(ssReq.RoomSubscriptions))
	for roomID := range ssReq.RoomSubscriptions {
		subscriptions = append(subscriptions, roomID)
	}
	sort.Strings(subscriptions)
	for _, roomID := range subscriptions {
		// Rooms the user is not allowed to see are ignored.
		entry, ok := roomList.byID[roomID]
		if !ok {
			continue
		}
		if !entry.inList && entry.membership != spec.Join {
			forgotten, forgottenErr := rp.isRoomForgotten(ctx, device.UserID, roomID)
			if forgottenErr != nil {
				return nil, nil, nil, false, forgottenErr
			}
			if forgotten {
				continue
			}
		}
		sub := ssReq.RoomSubscriptions[roomID]
		configFor(roomID).add(*sub.TimelineLimit, *sub.RequiredState)
	}

	var joinedRoomIDs []string
	for roomID := range configs {
		if roomList.byID[roomID].membership == spec.Join {
			joinedRoomIDs = append(joinedRoomIDs, roomID)
		}
	}
	bumpStamps, err := snapshot.MaxStreamPositionsForRooms(ctx, joinedRoomIDs, slidingSyncBumpEventTypes, currentPos.PDUPosition)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("snapshot.MaxStreamPositionsForRooms: %w", err)
	}

	r := &slidingSyncRequest{
		ctx: ctx, rp: rp, snapshot: snapshot, conn: conn, device: device, base: base, filters: filters,
		currentPos: currentPos.PDUPosition, ignored: ignoredSenders, bumpStamps: bumpStamps,
	}
	rooms = base.cloneRooms()
	for roomID, cfg := range configs {
		result, state, err := r.roomResult(roomList.byID[roomID], cfg, base.rooms[roomID])
		if err != nil {
			return nil, nil, nil, false, fmt.Errorf("room %s: %w", roomID, err)
		}
		rooms[roomID] = state
		if result != nil {
			res.Rooms[roomID] = result
			changed = true
		}
	}
	succeeded = true
	return res, rooms, listCounts, changed, nil
}
