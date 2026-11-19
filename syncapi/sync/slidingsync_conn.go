// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/element-hq/dendrite/syncapi/types"
)

// Limits for the per-connection state kept for sliding sync. The state is kept in memory,
// so these limits protect the server against clients creating lots of connections
// (see "Security considerations" in MSC4186).
const (
	// slidingSyncMaxConnectionsPerDevice is the maximum number of connections (distinct
	// conn_ids) per device. The least recently used connection is expired once exceeded.
	slidingSyncMaxConnectionsPerDevice = 10
	// slidingSyncMaxConnections is the maximum number of connections across all devices.
	slidingSyncMaxConnections = 10000
	// slidingSyncConnectionTTL is the time after which an unused connection is expired.
	slidingSyncConnectionTTL = time.Hour
	// slidingSyncMaxSnapshotsPerConnection is the maximum number of positions kept per connection.
	// A client only ever uses the latest position it processed, so only a few positions are needed
	// to handle retried and cancelled requests.
	slidingSyncMaxSnapshotsPerConnection = 5
	// slidingSyncMaxLazyMembers is the maximum number of lazily loaded members tracked per room.
	// If exceeded, the tracking is reset, meaning that memberships may be sent again.
	slidingSyncMaxLazyMembers = 1000
	// slidingSyncMaxCachedRooms is the maximum number of rooms for which immutable facts are cached
	// per connection.
	slidingSyncMaxCachedRooms = 20000
)

// errSlidingSyncUnknownPos is returned if a request uses a position which is unknown,
// expired or was not issued to the requesting device.
var errSlidingSyncUnknownPos = errors.New("unknown position")

// slidingSyncConnectionPositions is used to generate connection positions. It is shared between
// all connections so that positions are never reused, not even when a connection is recreated.
var slidingSyncConnectionPositions atomic.Int64

// slidingSyncRoomState is what a connection remembers about a room it has sent to the client.
// Values are never modified once they are part of a snapshot.
type slidingSyncRoomState struct {
	// The membership of the user as last sent to the client.
	Membership string
	// True if only stripped state was sent for the room (invites, knocks, rejected invites).
	Stripped bool
	// The ID of the event the stripped state was built from, used to detect changes.
	StrippedEventID string

	// The PDU stream position up to which the client has been sent the room.
	LastPos types.StreamPosition
	// The number of most recent timeline events (up to LastPos) the client is known to
	// have, or -1 if the client has all events up to LastPos.
	TimelineKnown int
	// The required state configuration which was last applied to the room.
	RequiredState []types.SlidingSyncRequiredState
	// The lazily loaded members which have been sent to the client.
	LazyMembers map[string]struct{}

	// The last values sent to the client.
	Name         *string
	Avatar       *string
	HeroesKey    string
	JoinedCount  int
	InvitedCount int
	IsDM         bool
	BumpStamp    int64
}

// slidingSyncSnapshot is the state of a connection at a given position.
// Snapshots are immutable once created.
type slidingSyncSnapshot struct {
	position   int64
	token      types.StreamingToken
	rooms      map[string]*slidingSyncRoomState
	listCounts map[string]int
}

// roomFacts are facts about a room which never change once known.
type roomFacts struct {
	hasRoomType bool
	roomType    *string
	isEncrypted bool
}

// everJoinedFact caches whether a user has ever been joined to a room for a given membership event.
type everJoinedFact struct {
	eventID    string
	everJoined bool
}

// slidingSyncConnection is a sliding sync connection of a single device.
type slidingSyncConnection struct {
	userID   string
	deviceID string
	connID   string
	now      func() time.Time

	mu         sync.Mutex
	snapshots  map[int64]*slidingSyncSnapshot
	lastUsed   time.Time
	facts      map[string]roomFacts
	everJoined map[string]everJoinedFact
}

// snapshot returns the snapshot for the given position, and forgets all snapshots
// before it, as the client must not use them anymore.
func (c *slidingSyncConnection) snapshot(pos types.SlidingSyncPosition) (*slidingSyncSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap, ok := c.snapshots[pos.ConnectionPosition]
	if !ok || snap.token != pos.StreamToken {
		return nil, errSlidingSyncUnknownPos
	}
	for p := range c.snapshots {
		if p < pos.ConnectionPosition {
			delete(c.snapshots, p)
		}
	}
	c.lastUsed = c.now()
	return snap, nil
}

// addSnapshot stores a new snapshot and returns its position.
func (c *slidingSyncConnection) addSnapshot(
	token types.StreamingToken, rooms map[string]*slidingSyncRoomState, listCounts map[string]int,
) types.SlidingSyncPosition {
	snap := &slidingSyncSnapshot{
		position:   slidingSyncConnectionPositions.Add(1),
		token:      token,
		rooms:      rooms,
		listCounts: listCounts,
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshots[snap.position] = snap
	for len(c.snapshots) > slidingSyncMaxSnapshotsPerConnection {
		oldest := snap.position
		for p := range c.snapshots {
			oldest = min(oldest, p)
		}
		delete(c.snapshots, oldest)
	}
	c.lastUsed = c.now()
	return types.SlidingSyncPosition{ConnectionPosition: snap.position, StreamToken: token}
}

// getFacts returns the cached facts for a room.
func (c *slidingSyncConnection) getFacts(roomID string) roomFacts {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.facts[roomID]
}

// setFacts caches facts for a room.
func (c *slidingSyncConnection) setFacts(roomID string, facts roomFacts) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.facts) >= slidingSyncMaxCachedRooms {
		clear(c.facts)
	}
	c.facts[roomID] = facts
}

// getEverJoined returns the cached "ever joined" value for the given membership event.
func (c *slidingSyncConnection) getEverJoined(roomID, eventID string) (everJoined, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fact, ok := c.everJoined[roomID]
	if !ok || fact.eventID != eventID {
		return false, false
	}
	return fact.everJoined, true
}

// setEverJoined caches the "ever joined" value for the given membership event.
func (c *slidingSyncConnection) setEverJoined(roomID, eventID string, everJoined bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.everJoined) >= slidingSyncMaxCachedRooms {
		clear(c.everJoined)
	}
	c.everJoined[roomID] = everJoinedFact{eventID: eventID, everJoined: everJoined}
}

type slidingSyncConnectionKey struct {
	userID   string
	deviceID string
	connID   string
}

// slidingSyncConnections holds all sliding sync connections.
type slidingSyncConnections struct {
	mu    sync.Mutex
	conns map[slidingSyncConnectionKey]*slidingSyncConnection
	// Overridable for tests.
	now func() time.Time
}

func newSlidingSyncConnections() *slidingSyncConnections {
	return &slidingSyncConnections{
		conns: make(map[slidingSyncConnectionKey]*slidingSyncConnection),
		now:   time.Now,
	}
}

// get returns the connection and snapshot for the given position. The connection is looked up
// using the user ID and device ID of the requester, so positions issued to other devices are
// never accepted.
func (s *slidingSyncConnections) get(userID, deviceID, connID, pos string) (*slidingSyncConnection, *slidingSyncSnapshot, error) {
	position, err := types.NewSlidingSyncPositionFromString(pos)
	if err != nil {
		return nil, nil, errSlidingSyncUnknownPos
	}
	s.mu.Lock()
	conn, ok := s.conns[slidingSyncConnectionKey{userID: userID, deviceID: deviceID, connID: connID}]
	if ok && s.expired(conn) {
		s.removeLocked(conn)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return nil, nil, errSlidingSyncUnknownPos
	}
	snap, err := conn.snapshot(position)
	if err != nil {
		return nil, nil, err
	}
	return conn, snap, nil
}

// create creates a new connection, replacing any existing connection with the same conn_id.
func (s *slidingSyncConnections) create(userID, deviceID, connID string) (*slidingSyncConnection, *slidingSyncSnapshot) {
	conn := &slidingSyncConnection{
		userID:     userID,
		deviceID:   deviceID,
		connID:     connID,
		now:        s.now,
		snapshots:  make(map[int64]*slidingSyncSnapshot),
		lastUsed:   s.now(),
		facts:      make(map[string]roomFacts),
		everJoined: make(map[string]everJoinedFact),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := slidingSyncConnectionKey{userID: userID, deviceID: deviceID, connID: connID}
	if existing, ok := s.conns[key]; ok {
		s.removeLocked(existing)
	}
	// Enforce the per-device and global limits by expiring the least recently used connections.
	var deviceConns []*slidingSyncConnection
	for _, c := range s.conns {
		if c.userID == userID && c.deviceID == deviceID {
			deviceConns = append(deviceConns, c)
		}
	}
	for len(deviceConns) >= slidingSyncMaxConnectionsPerDevice {
		lru := s.leastRecentlyUsed(deviceConns)
		s.removeLocked(lru)
		deviceConns = s.without(deviceConns, lru)
	}
	if len(s.conns) >= slidingSyncMaxConnections {
		s.expireLocked()
	}
	for len(s.conns) >= slidingSyncMaxConnections {
		all := make([]*slidingSyncConnection, 0, len(s.conns))
		for _, c := range s.conns {
			all = append(all, c)
		}
		s.removeLocked(s.leastRecentlyUsed(all))
	}
	s.conns[key] = conn
	return conn, &slidingSyncSnapshot{
		rooms:      map[string]*slidingSyncRoomState{},
		listCounts: map[string]int{},
	}
}

// expire removes all connections which have not been used within the TTL.
func (s *slidingSyncConnections) expire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
}

func (s *slidingSyncConnections) expireLocked() {
	for _, c := range s.conns {
		if s.expired(c) {
			s.removeLocked(c)
		}
	}
}

func (s *slidingSyncConnections) expired(c *slidingSyncConnection) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return s.now().Sub(c.lastUsed) > slidingSyncConnectionTTL
}

func (s *slidingSyncConnections) removeLocked(c *slidingSyncConnection) {
	key := slidingSyncConnectionKey{userID: c.userID, deviceID: c.deviceID, connID: c.connID}
	if s.conns[key] == c {
		delete(s.conns, key)
	}
}

func (s *slidingSyncConnections) leastRecentlyUsed(conns []*slidingSyncConnection) *slidingSyncConnection {
	var lru *slidingSyncConnection
	var lruTime time.Time
	for _, c := range conns {
		c.mu.Lock()
		lastUsed := c.lastUsed
		c.mu.Unlock()
		if lru == nil || lastUsed.Before(lruTime) {
			lru, lruTime = c, lastUsed
		}
	}
	return lru
}

func (s *slidingSyncConnections) without(conns []*slidingSyncConnection, remove *slidingSyncConnection) []*slidingSyncConnection {
	result := conns[:0]
	for _, c := range conns {
		if c != remove {
			result = append(result, c)
		}
	}
	return result
}

// count returns the number of connections. Used in tests.
func (s *slidingSyncConnections) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// cleanup periodically expires unused connections.
func (s *slidingSyncConnections) cleanup() {
	for {
		time.Sleep(time.Minute)
		s.expire()
	}
}

// cloneRooms returns a shallow copy of the rooms of a snapshot, which can be modified
// to build the rooms of the next snapshot.
func (snap *slidingSyncSnapshot) cloneRooms() map[string]*slidingSyncRoomState {
	return maps.Clone(snap.rooms)
}
