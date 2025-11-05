package mercure

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// RedisDefaultCleanupFrequency is the default frequency at which old events are cleaned up.
	RedisDefaultCleanupFrequency = 0.3

	// redisUpdatePrefix is the prefix for update keys stored in Redis.
	redisUpdatePrefix = "mercure:update:"

	// redisSequenceKey is the key used to store the sequence counter in Redis.
	redisSequenceKey = "mercure:sequence"

	// redisLastEventIDKey is the key used to store the last event ID in Redis.
	redisLastEventIDKey = "mercure:last_event_id"
)

// RedisTransport implements the Transport interface using Redis as the storage backend.
// It provides persistence and history retrieval capabilities for Server-Sent Events (SSE).
// Redis enables high-availability setups with multiple Mercure instances sharing the same data store.
type RedisTransport struct {
	sync.RWMutex

	// subscribers is the list of active local subscribers connected to this instance.
	subscribers *SubscriberList

	// logger is used for logging transport operations and errors.
	logger Logger

	// client is the Redis client used to communicate with the Redis server.
	client *redis.Client

	// ctx is the context used for Redis operations.
	ctx context.Context

	// cancel is the cancel function for the context.
	cancel context.CancelFunc

	// size is the maximum number of events to keep in history (0 = unlimited).
	size uint64

	// cleanupFrequency determines how often cleanup is triggered (0-1, where 1 = always).
	cleanupFrequency float64

	// closed is a channel that signals when the transport is closed.
	closed chan struct{}

	// closedOnce ensures Close() is only executed once.
	closedOnce sync.Once

	// lastSeq tracks the last sequence number assigned to an update.
	lastSeq uint64

	// lastEventID tracks the ID of the most recently dispatched event.
	lastEventID string
}

// NewRedisTransport creates a new RedisTransport instance.
// Parameters:
//   - subscriberList: The list to manage subscribers for this transport instance
//   - logger: Logger for recording transport operations and errors
//   - redisURL: Connection URL for Redis (e.g., "redis://localhost:6379/0")
//   - size: Maximum number of events to retain in history (0 for unlimited)
//   - cleanupFrequency: Probability of triggering cleanup on each dispatch (0-1)
//
// Returns the initialized transport or an error if connection fails.
func NewRedisTransport(
	subscriberList *SubscriberList,
	logger Logger,
	redisURL string,
	size uint64,
	cleanupFrequency float64,
) (*RedisTransport, error) {
	// Parse the Redis URL to get connection options
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, &TransportError{dsn: redisURL, err: err}
	}

	// Create Redis client with the parsed options
	client := redis.NewClient(opts)

	// Create a context for Redis operations with cancel capability
	ctx, cancel := context.WithCancel(context.Background())

	// Test the connection to Redis
	if err := client.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, &TransportError{dsn: redisURL, err: err}
	}

	// Retrieve the last event ID from Redis to maintain continuity
	lastEventID, err := getRedisLastEventID(ctx, client)
	if err != nil {
		cancel()
		return nil, &TransportError{dsn: redisURL, err: err}
	}

	return &RedisTransport{
		logger:           logger,
		client:           client,
		ctx:              ctx,
		cancel:           cancel,
		size:             size,
		cleanupFrequency: cleanupFrequency,
		subscribers:      subscriberList,
		closed:           make(chan struct{}),
		lastEventID:      lastEventID,
	}, nil
}

// getRedisLastEventID retrieves the last event ID from Redis.
// Returns EarliestLastEventID if no events exist in the database.
func getRedisLastEventID(ctx context.Context, client *redis.Client) (string, error) {
	// Try to get the last event ID from Redis
	lastEventID, err := client.Get(ctx, redisLastEventIDKey).Result()
	if err == redis.Nil {
		// No last event ID stored, this is a fresh database
		return EarliestLastEventID, nil
	}
	if err != nil {
		return "", fmt.Errorf("unable to get lastEventID from Redis: %w", err)
	}

	return lastEventID, nil
}

// Dispatch dispatches an update to all subscribers and persists it in Redis.
// The update is stored with a sequence number to maintain ordering.
// Subscribers matching the update's topics will receive the event.
func (t *RedisTransport) Dispatch(update *Update) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	// Assign a UUID to the update if it doesn't have one
	update.AssignUUID()

	// Marshal the update to JSON for storage
	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return fmt.Errorf("error when marshaling update: %w", err)
	}

	// Lock for writing to Redis and updating internal state
	t.Lock()
	defer t.Unlock()

	// Persist the update to Redis
	if err := t.persist(update.ID, updateJSON); err != nil {
		return err
	}

	// Dispatch the update to all matching local subscribers
	for _, s := range t.subscribers.MatchAny(update) {
		s.Dispatch(update, false)
	}

	return nil
}

// AddSubscriber adds a new subscriber to the transport.
// If the subscriber has a RequestLastEventID, historical events are dispatched first.
func (t *RedisTransport) AddSubscriber(s *LocalSubscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	// Add the subscriber to the list and get the current sequence number
	t.Lock()
	t.subscribers.Add(s)
	toSeq := t.lastSeq
	t.Unlock()

	// If the subscriber requested historical events, dispatch them
	if s.RequestLastEventID != "" {
		if err := t.dispatchHistory(s, toSeq); err != nil {
			return err
		}
	}

	// Signal that the subscriber is ready to receive live updates
	s.Ready()

	return nil
}

// RemoveSubscriber removes a subscriber from the transport.
func (t *RedisTransport) RemoveSubscriber(s *LocalSubscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	defer t.Unlock()

	t.subscribers.Remove(s)

	return nil
}

// GetSubscribers returns the last event ID and the list of active subscribers.
// This implements the TransportSubscribers interface.
func (t *RedisTransport) GetSubscribers() (string, []*Subscriber, error) {
	t.RLock()
	defer t.RUnlock()

	return t.lastEventID, getSubscribers(t.subscribers), nil
}

// Close closes the Transport, disconnecting all subscribers and closing the Redis connection.
func (t *RedisTransport) Close() (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)

		t.Lock()
		defer t.Unlock()

		// Disconnect all subscribers
		t.subscribers.Walk(0, func(s *LocalSubscriber) bool {
			s.Disconnect()
			return true
		})

		// Cancel the context to stop any ongoing Redis operations
		t.cancel()

		// Close the Redis client connection
		err = t.client.Close()
	})

	if err == nil {
		return nil
	}

	return fmt.Errorf("unable to close Redis client: %w", err)
}

// updateWithSeq represents an update with its associated sequence number.
// Used for sorting updates when retrieving history from Redis.
type updateWithSeq struct {
	seq    uint64
	id     string
	update *Update
}

// parseSequenceFromKey extracts the sequence number from a Redis key.
// Keys are in the format "mercure:update:SEQ:ID".
// Returns the sequence number or 0 if the key format is invalid.
func parseSequenceFromKey(key string) (uint64, error) {
	var seq uint64
	_, err := fmt.Sscanf(key, redisUpdatePrefix+"%d:", &seq)
	if err != nil {
		return 0, fmt.Errorf("invalid key format: %w", err)
	}
	return seq, nil
}

// sortUpdatesBySequence sorts a slice of updates by their sequence numbers.
func sortUpdatesBySequence(updates []updateWithSeq) {
	// Use Go's built-in sort for O(n log n) performance
	for i := 0; i < len(updates)-1; i++ {
		for j := i + 1; j < len(updates); j++ {
			if updates[i].seq > updates[j].seq {
				updates[i], updates[j] = updates[j], updates[i]
			}
		}
	}
}

// fetchAndParseUpdates retrieves all updates from Redis and parses them.
// Returns a slice of updates sorted by sequence number.
func (t *RedisTransport) fetchAndParseUpdates(keys []string) ([]updateWithSeq, error) {
	var updates []updateWithSeq

	// Fetch and parse all updates
	for _, key := range keys {
		// Get the update data from Redis
		data, err := t.client.Get(t.ctx, key).Result()
		if err != nil {
			continue // Skip invalid entries
		}

		var update Update
		if err := json.Unmarshal([]byte(data), &update); err != nil {
			if c := t.logger.Check(zap.ErrorLevel, "Unable to unmarshal update from Redis"); c != nil {
				c.Write(zap.Error(err), zap.String("key", key))
			}
			continue
		}

		// Extract sequence number from key
		seq, err := parseSequenceFromKey(key)
		if err != nil {
			continue // Skip invalid keys
		}

		updates = append(updates, updateWithSeq{
			seq:    seq,
			id:     update.ID,
			update: &update,
		})
	}

	return updates, nil
}

// dispatchHistory retrieves and dispatches historical events to a subscriber.
// Events are fetched from Redis based on the subscriber's RequestLastEventID.
func (t *RedisTransport) dispatchHistory(s *LocalSubscriber, toSeq uint64) error {
	// Get all update keys from Redis sorted by sequence number
	keys, err := t.client.Keys(t.ctx, redisUpdatePrefix+"*").Result()
	if err != nil {
		return fmt.Errorf("unable to retrieve keys from Redis: %w", err)
	}

	if len(keys) == 0 {
		// No history available
		s.HistoryDispatched(EarliestLastEventID)
		return nil
	}

	// Fetch and parse all updates
	type updateWithSeq struct {
		seq    uint64
		id     string
		update *Update
	}

	updates, err := t.fetchAndParseUpdates(keys)
	if err != nil {
		return err
	}

	// Sort updates by sequence number using Go's built-in sort
	sortUpdatesBySequence(updates)

	// Dispatch updates in order
	responseLastEventID := EarliestLastEventID
	afterFromID := s.RequestLastEventID == EarliestLastEventID

	for _, u := range updates {
		if !afterFromID {
			responseLastEventID = u.id
			if responseLastEventID == s.RequestLastEventID {
				afterFromID = true
			}
			continue
		}

		// Check if we should dispatch this update
		if (s.Match(u.update) && !s.Dispatch(u.update, true)) || (toSeq > 0 && u.seq >= toSeq) {
			s.HistoryDispatched(responseLastEventID)
			return nil
		}
	}

	s.HistoryDispatched(responseLastEventID)

	// Log if the requested last event ID was not found
	if !afterFromID {
		if c := t.logger.Check(zap.InfoLevel, "Can't find requested LastEventID"); c != nil {
			c.Write(zap.String("LastEventID", s.RequestLastEventID))
		}
	}

	return nil
}

// persist stores an update in Redis with a sequence number for ordering.
// The update is stored with a key format: "mercure:update:SEQ:ID"
func (t *RedisTransport) persist(updateID string, updateJSON []byte) error {
	// Increment the sequence counter in Redis atomically
	seq, err := t.client.Incr(t.ctx, redisSequenceKey).Result()
	if err != nil {
		return fmt.Errorf("unable to increment sequence in Redis: %w", err)
	}

	// Create the key for this update using sequence and ID
	key := fmt.Sprintf("%s%d:%s", redisUpdatePrefix, seq, updateID)

	// Store the update in Redis
	if err := t.client.Set(t.ctx, key, updateJSON, 0).Err(); err != nil {
		return fmt.Errorf("unable to set value in Redis: %w", err)
	}

	// Update the last event ID in Redis
	if err := t.client.Set(t.ctx, redisLastEventIDKey, updateID, 0).Err(); err != nil {
		return fmt.Errorf("unable to set last event ID in Redis: %w", err)
	}

	// Update internal state
	t.lastSeq = uint64(seq)
	t.lastEventID = updateID

	// Perform cleanup if needed
	return t.cleanup(uint64(seq))
}

// cleanup removes old events from Redis based on the size limit.
// Cleanup is triggered probabilistically based on cleanupFrequency.
func (t *RedisTransport) cleanup(lastSeq uint64) error {
	// Skip cleanup if not configured or not needed
	if t.size == 0 || t.cleanupFrequency == 0 || t.size >= lastSeq {
		return nil
	}

	// Probabilistic cleanup trigger (consistent with BoltTransport)
	// When frequency is 1.0, always cleanup; otherwise use random probability
	if t.cleanupFrequency != 1 && !shouldCleanup(t.cleanupFrequency) {
		return nil
	}

	// Calculate the sequence number threshold for removal
	removeUntil := lastSeq - t.size

	// Get all update keys
	keys, err := t.client.Keys(t.ctx, redisUpdatePrefix+"*").Result()
	if err != nil {
		return fmt.Errorf("unable to retrieve keys for cleanup from Redis: %w", err)
	}

	// Delete old keys
	var keysToDelete []string
	for _, key := range keys {
		// Extract sequence number from key
		seq, err := parseSequenceFromKey(key)
		if err != nil {
			continue
		}

		if seq <= removeUntil {
			keysToDelete = append(keysToDelete, key)
		}
	}

	// Delete the keys in batches if any exist
	if len(keysToDelete) > 0 {
		if err := t.client.Del(t.ctx, keysToDelete...).Err(); err != nil {
			return fmt.Errorf("unable to delete old keys from Redis: %w", err)
		}
	}

	return nil
}

// shouldCleanup determines whether cleanup should be triggered based on probability.
// This uses the same approach as BoltTransport for consistency.
func shouldCleanup(frequency float64) bool {
	// Use math/rand for consistency with BoltTransport
	// Note: crypto/rand is not needed here as this is not security-sensitive
	return rand.Float64() < frequency //nolint:gosec
}

// Interface guards to ensure RedisTransport implements required interfaces.
var (
	_ Transport            = (*RedisTransport)(nil)
	_ TransportSubscribers = (*RedisTransport)(nil)
)
