// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"github.com/element-hq/dendrite/syncapi/internal"
	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/types"
	userapi "github.com/element-hq/dendrite/userapi/api"
)

// slidingSyncExtensions are the extensions enabled for a sliding sync request.
type slidingSyncExtensions struct {
	// True if the request was made to the unstable endpoint, which uses the
	// response formats of the experimental implementations.
	unstableEndpoint bool

	// The to-device extension (MSC4538), if enabled.
	toDeviceKey   string
	toDeviceLimit int
	// The position the client acknowledged, or nil if the client hasn't received any messages yet.
	toDeviceSince *types.StreamPosition

	// The end-to-end encryption extension (MSC4543), if enabled.
	e2eeKey string
}

// errSlidingSyncInvalidSince is returned if a to-device `since` was not issued by the server.
var errSlidingSyncInvalidSince = fmt.Errorf("invalid to-device since token")

// parseSlidingSyncExtensions parses the extensions of a request. Unknown extensions are ignored.
func parseSlidingSyncExtensions(raw map[string]json.RawMessage, unstableEndpoint bool) (*slidingSyncExtensions, *util.JSONResponse) {
	ext := &slidingSyncExtensions{unstableEndpoint: unstableEndpoint}
	toDeviceKeys := []string{types.SlidingSyncExtensionToDevice}
	e2eeKeys := []string{types.SlidingSyncExtensionE2EE}
	if !unstableEndpoint {
		// The unstable keys of MSC4538 and MSC4543 are only used on the stable endpoint.
		toDeviceKeys = append(toDeviceKeys, types.SlidingSyncExtensionToDeviceUnstable)
		e2eeKeys = append(e2eeKeys, types.SlidingSyncExtensionE2EEUnstable)
	}
	badJSON := func(key string) *util.JSONResponse {
		return &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(fmt.Sprintf("Invalid configuration for extension %q", key)),
		}
	}

	for _, key := range toDeviceKeys {
		data, ok := raw[key]
		if !ok {
			continue
		}
		var cfg types.SlidingSyncToDeviceRequest
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, badJSON(key)
		}
		if !cfg.Enabled {
			continue
		}
		limit := types.SlidingSyncDefaultToDeviceLimit
		if cfg.Limit != nil {
			if *cfg.Limit <= 0 {
				return nil, &util.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.BadJSON("The to-device limit must be a positive integer"),
				}
			}
			limit = min(*cfg.Limit, types.SlidingSyncMaxToDeviceLimit)
		}
		if cfg.Since != nil {
			since, err := strconv.ParseInt(*cfg.Since, 10, 64)
			if err != nil || since < 0 || strconv.FormatInt(since, 10) != *cfg.Since {
				return nil, invalidSinceResponse()
			}
			pos := types.StreamPosition(since)
			ext.toDeviceSince = &pos
		}
		ext.toDeviceKey = key
		ext.toDeviceLimit = limit
		break
	}

	for _, key := range e2eeKeys {
		data, ok := raw[key]
		if !ok {
			continue
		}
		var cfg types.SlidingSyncE2EERequest
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, badJSON(key)
		}
		if cfg.Enabled {
			ext.e2eeKey = key
			break
		}
	}
	return ext, nil
}

func invalidSinceResponse() *util.JSONResponse {
	return &util.JSONResponse{
		Code: http.StatusBadRequest,
		JSON: spec.InvalidParam("Unknown to-device since token"),
	}
}

// acknowledgeToDevice validates the to-device `since` token and deletes the messages it
// acknowledges. Messages are only ever deleted for the requesting device.
func (rp *RequestPool) acknowledgeToDevice(ctx context.Context, device *userapi.Device, ext *slidingSyncExtensions) error {
	if ext.toDeviceKey == "" || ext.toDeviceSince == nil {
		return nil
	}
	snapshot, err := rp.db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("NewDatabaseSnapshot: %w", err)
	}
	maxPos, err := snapshot.AllocatedSendToDevicePosition(ctx)
	_ = snapshot.Rollback()
	if err != nil {
		return fmt.Errorf("snapshot.AllocatedSendToDevicePosition: %w", err)
	}
	// Tokens are positions of send-to-device messages, so the server can't have issued a
	// token beyond the highest position allocated to a message.
	if *ext.toDeviceSince > maxPos {
		return errSlidingSyncInvalidSince
	}
	return rp.db.CleanSendToDeviceUpdates(ctx, device.UserID, device.ID, *ext.toDeviceSince)
}

// extensionResults calculates the results of the enabled extensions. It returns true if the
// results contain updates the client must be woken up for.
func (rp *RequestPool) extensionResults(
	ctx context.Context, snapshot storage.DatabaseTransaction, device *userapi.Device, base *slidingSyncSnapshot,
	ext *slidingSyncExtensions, currentPos types.StreamingToken, res *types.SlidingSyncResponse,
) (bool, error) {
	changed := false
	if ext.toDeviceKey != "" {
		hasMessages, err := rp.toDeviceResult(ctx, snapshot, device, ext, res)
		if err != nil {
			return false, err
		}
		changed = changed || hasMessages
	}
	if ext.e2eeKey != "" {
		hasDeviceListChanges, err := rp.e2eeResult(ctx, snapshot, device, base, ext, currentPos, res)
		if err != nil {
			return false, err
		}
		changed = changed || hasDeviceListChanges
	}
	return changed, nil
}

// toDeviceResult adds the to-device messages queued for the device after `since`.
func (rp *RequestPool) toDeviceResult(
	ctx context.Context, snapshot storage.DatabaseTransaction, device *userapi.Device,
	ext *slidingSyncExtensions, res *types.SlidingSyncResponse,
) (bool, error) {
	var since types.StreamPosition
	if ext.toDeviceSince != nil {
		since = *ext.toDeviceSince
	}
	// Only read as many messages as are returned, so that a large backlog isn't loaded at once.
	// The token is the position of the last message read. It must not be advanced beyond that,
	// as messages which aren't visible to this snapshot yet could be acknowledged otherwise.
	nextBatch, events, err := snapshot.SendToDeviceMessagesAfter(ctx, device.UserID, device.ID, since, ext.toDeviceLimit)
	if err != nil {
		return false, fmt.Errorf("snapshot.SendToDeviceMessagesAfter: %w", err)
	}
	messages := make([]gomatrixserverlib.SendToDeviceEvent, 0, len(events))
	for _, ev := range events {
		messages = append(messages, ev.SendToDeviceEvent)
	}
	nextBatchStr := strconv.FormatInt(int64(nextBatch), 10)

	var data []byte
	switch {
	case ext.unstableEndpoint:
		// The experimental implementations always include the section.
		data, err = json.Marshal(types.SlidingSyncToDeviceLegacyResponse{NextBatch: nextBatchStr, Events: messages})
	case len(messages) > 0 || nextBatch != since:
		// Also sent if only malformed messages were read, so that the client acknowledges them.
		data, err = json.Marshal(types.SlidingSyncToDeviceResponse{NextBatch: nextBatchStr, Messages: messages})
	}
	if err != nil {
		return false, err
	}
	if data != nil {
		res.Extensions[ext.toDeviceKey] = data
	}
	return len(messages) > 0, nil
}

// e2eeResult adds the device list changes since the request's position, and the one-time
// and fallback key state of the device.
func (rp *RequestPool) e2eeResult(
	ctx context.Context, snapshot storage.DatabaseTransaction, device *userapi.Device, base *slidingSyncSnapshot,
	ext *slidingSyncExtensions, currentPos types.StreamingToken, res *types.SlidingSyncResponse,
) (bool, error) {
	result := types.SlidingSyncE2EEResponse{
		DeviceOneTimeKeysCount:       map[string]int{},
		DeviceUnusedFallbackKeyTypes: []string{},
	}
	// The response to a request without a position doesn't contain device list changes.
	hasChanges := false
	if !base.token.IsEmpty() {
		deviceLists, err := rp.deviceListChanges(ctx, snapshot, device, base.token, currentPos)
		if err != nil {
			return false, fmt.Errorf("deviceListChanges: %w", err)
		}
		if deviceLists != nil && (len(deviceLists.Changed) > 0 || len(deviceLists.Left) > 0) {
			result.DeviceLists = deviceLists
			hasChanges = true
		}
	}

	keys := types.NewResponse()
	if err := internal.DeviceOTKCounts(ctx, rp.userAPI, device.UserID, device.ID, keys); err != nil {
		return false, fmt.Errorf("DeviceOTKCounts: %w", err)
	}
	for algorithm, count := range keys.DeviceListsOTKCount {
		result.DeviceOneTimeKeysCount[algorithm] = count
	}
	result.DeviceUnusedFallbackKeyTypes = append(result.DeviceUnusedFallbackKeyTypes, keys.DeviceListsUnusedFallbackAlgorithms...)

	// The section is always included, which the extension permits. That way the key state
	// is never lost if a response doesn't reach the client.
	data, err := json.Marshal(result)
	if err != nil {
		return false, err
	}
	res.Extensions[ext.e2eeKey] = data
	return hasChanges, nil
}
