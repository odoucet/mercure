package mercure

import (
	"context"
	"os"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// getTestRedisURL returns the Redis URL for testing.
// It checks the REDIS_URL environment variable first, otherwise uses a default.
func getTestRedisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://localhost:6379/15" // Use DB 15 for testing to avoid conflicts
}

// isRedisAvailable checks if Redis is available for testing.
func isRedisAvailable(t *testing.T) bool {
	t.Helper()

	opts, err := redis.ParseURL(getTestRedisURL())
	if err != nil {
		return false
	}

	client := redis.NewClient(opts)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return client.Ping(ctx).Err() == nil
}

// createRedisTransport creates a new RedisTransport for testing.
// It cleans up the test database before creating the transport.
func createRedisTransport(t *testing.T, size uint64, cleanupFrequency float64) *RedisTransport {
	t.Helper()

	if !isRedisAvailable(t) {
		t.Skip("Redis is not available for testing")
	}

	if cleanupFrequency == 0 {
		cleanupFrequency = RedisDefaultCleanupFrequency
	}

	redisURL := getTestRedisURL()

	// Clean up any existing test data
	opts, err := redis.ParseURL(redisURL)
	require.NoError(t, err)
	
	client := redis.NewClient(opts)
	ctx := context.Background()
	
	// Flush the test database to ensure clean state
	require.NoError(t, client.FlushDB(ctx).Err())
	client.Close()

	// Create the transport
	transport, err := NewRedisTransport(NewSubscriberList(0), zap.NewNop(), redisURL, size, cleanupFrequency)
	require.NoError(t, err)

	t.Cleanup(func() {
		// Clean up after the test
		if transport.client != nil {
			_ = transport.client.FlushDB(transport.ctx).Err()
		}
		require.NoError(t, transport.Close())
	})

	return transport
}

// TestRedisTransportHistory tests that historical events are correctly retrieved.
func TestRedisTransportHistory(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Dispatch 10 updates
	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		require.NoError(t, transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		}))
	}

	// Create a subscriber requesting history from event #8
	s := NewLocalSubscriber("8", transport.logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)

	require.NoError(t, transport.AddSubscriber(s))

	var count int

	// Verify that we receive events #9 and #10
	for {
		u := <-s.Receive()
		// The reading loop must read the #9 and #10 messages
		assert.Equal(t, strconv.Itoa(9+count), u.ID)

		count++
		if count == 2 {
			return
		}
	}
}

// TestRedisTransportLogsBogusLastEventID tests that an invalid last event ID is logged.
func TestRedisTransportLogsBogusLastEventID(t *testing.T) {

	sink, logger := newTestLogger(t)
	t.Cleanup(sink.Reset)

	transport := createRedisTransport(t, 0, 0)
	transport.logger = logger

	// Make sure the database is not empty
	topics := []string{"https://example.com/foo"}
	require.NoError(t, transport.Dispatch(&Update{
		Event:  Event{ID: "1"},
		Topics: topics,
	}))

	// Subscribe with a bogus last event ID
	s := NewLocalSubscriber("711131", logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)

	require.NoError(t, transport.AddSubscriber(s))

	log := sink.String()
	assert.Contains(t, log, `"LastEventID":"711131"`)
}

// TestRedisTopicSelectorHistory tests that topic selectors work correctly with history.
func TestRedisTopicSelectorHistory(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Dispatch various updates with different topics and privacy settings
	require.NoError(t, transport.Dispatch(&Update{Topics: []string{"https://example.com/subscribed"}, Event: Event{ID: "1"}}))
	require.NoError(t, transport.Dispatch(&Update{Topics: []string{"https://example.com/not-subscribed"}, Event: Event{ID: "2"}}))
	require.NoError(t, transport.Dispatch(&Update{Topics: []string{"https://example.com/subscribed-public-only"}, Private: true, Event: Event{ID: "3"}}))
	require.NoError(t, transport.Dispatch(&Update{Topics: []string{"https://example.com/subscribed-public-only"}, Event: Event{ID: "4"}}))

	// Create a subscriber with topic filtering
	s := NewLocalSubscriber(EarliestLastEventID, transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/subscribed", "https://example.com/subscribed-public-only"}, []string{"https://example.com/subscribed"})

	require.NoError(t, transport.AddSubscriber(s))

	// Should receive event #1 (subscribed, public) and event #4 (subscribed-public-only, public)
	assert.Equal(t, "1", (<-s.Receive()).ID)
	assert.Equal(t, "4", (<-s.Receive()).ID)
}

// TestRedisTransportRetrieveAllHistory tests retrieving all historical events.
func TestRedisTransportRetrieveAllHistory(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Dispatch 10 updates
	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		require.NoError(t, transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		}))
	}

	// Subscribe from the beginning
	s := NewLocalSubscriber(EarliestLastEventID, transport.logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)
	require.NoError(t, transport.AddSubscriber(s))

	var count int

	// Verify we receive all 10 messages in order
	for {
		u := <-s.Receive()
		// The reading loop must read all messages
		count++
		assert.Equal(t, strconv.Itoa(count), u.ID)

		if count == 10 {
			break
		}
	}

	assert.Equal(t, 10, count)
}

// TestRedisTransportHistoryAndLive tests that both historical and live events are received.
func TestRedisTransportHistoryAndLive(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Dispatch 10 historical updates
	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		require.NoError(t, transport.Dispatch(&Update{
			Topics: topics,
			Event:  Event{ID: strconv.Itoa(i)},
		}))
	}

	// Create a subscriber requesting history from event #8
	s := NewLocalSubscriber("8", transport.logger, &TopicSelectorStore{})
	s.SetTopics(topics, nil)
	require.NoError(t, transport.AddSubscriber(s))

	synctest.Test(t, func(t *testing.T) {
		go func() {
			var count int

			for {
				u := <-s.Receive()

				// The reading loop must read the #9, #10 and #11 messages
				assert.Equal(t, strconv.Itoa(9+count), u.ID)

				count++
				if count == 3 {
					return
				}
			}
		}()

		// Dispatch a new live event
		require.NoError(t, transport.Dispatch(&Update{
			Event:  Event{ID: "11"},
			Topics: topics,
		}))

		synctest.Wait()
	})
}

// TestRedisTransportPurgeHistory tests that old events are cleaned up when size limit is reached.
func TestRedisTransportPurgeHistory(t *testing.T) {

	transport := createRedisTransport(t, 5, 1) // Keep only 5 events, always cleanup

	// Dispatch 12 updates
	for i := range 12 {
		require.NoError(t, transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: []string{"https://example.com/foo"},
		}))
	}

	// Count the number of keys in Redis
	keys, err := transport.client.Keys(transport.ctx, redisUpdatePrefix+"*").Result()
	require.NoError(t, err)
	
	// Should have approximately 5 events (may be slightly more due to async cleanup)
	assert.LessOrEqual(t, len(keys), 7, "Too many events retained after cleanup")
	assert.GreaterOrEqual(t, len(keys), 5, "Too few events retained after cleanup")
}

// TestRedisTransportDoNotDispatchUntilListen tests that updates before subscription aren't received.
func TestRedisTransportDoNotDispatchUntilListen(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)
	assert.Implements(t, (*Transport)(nil), transport)

	// Add a subscriber without requesting history
	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	require.NoError(t, transport.AddSubscriber(s))

	synctest.Test(t, func(t *testing.T) {
		go func() {
			for range s.Receive() {
				t.Fail()
			}
		}()

		s.Disconnect()

		synctest.Wait()
	})
}

// TestRedisTransportDispatch tests basic event dispatching to subscribers.
func TestRedisTransportDispatch(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)
	assert.Implements(t, (*Transport)(nil), transport)

	// Create a subscriber with public and private topic subscriptions
	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/foo", "https://example.com/private"}, []string{"https://example.com/private"})

	require.NoError(t, transport.AddSubscriber(s))

	// Dispatch an update to a topic the subscriber is not subscribed to
	notSubscribed := &Update{Topics: []string{"not-subscribed"}}
	require.NoError(t, transport.Dispatch(notSubscribed))

	// Dispatch a private update to a public-only topic (should not be received)
	subscribedNotAuthorized := &Update{Topics: []string{"https://example.com/foo"}, Private: true}
	require.NoError(t, transport.Dispatch(subscribedNotAuthorized))

	// Dispatch a public update (should be received)
	public := &Update{Topics: s.SubscribedTopics}
	require.NoError(t, transport.Dispatch(public))

	assert.Equal(t, public, <-s.Receive())

	// Dispatch a private update to an authorized topic (should be received)
	private := &Update{Topics: s.AllowedPrivateTopics, Private: true}
	require.NoError(t, transport.Dispatch(private))

	assert.Equal(t, private, <-s.Receive())
}

// TestRedisTransportClosed tests that operations fail after the transport is closed.
func TestRedisTransportClosed(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)
	assert.Implements(t, (*Transport)(nil), transport)

	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/foo"}, nil)
	require.NoError(t, transport.AddSubscriber(s))

	// Close the transport
	require.NoError(t, transport.Close())
	
	// Operations should fail after close
	require.Error(t, transport.AddSubscriber(s))

	assert.Equal(t, transport.Dispatch(&Update{Topics: s.SubscribedTopics}), ErrClosedTransport)

	// Subscriber should be disconnected
	_, ok := <-s.Receive()
	assert.False(t, ok)
}

// TestRedisCleanDisconnectedSubscribers tests that disconnected subscribers are properly removed.
func TestRedisCleanDisconnectedSubscribers(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Add two subscribers
	s1 := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s1.SetTopics([]string{"foo"}, []string{})
	require.NoError(t, transport.AddSubscriber(s1))

	s2 := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s2.SetTopics([]string{"foo"}, []string{})
	require.NoError(t, transport.AddSubscriber(s2))

	assert.Equal(t, 2, transport.subscribers.Len())

	// Remove first subscriber
	s1.Disconnect()
	require.NoError(t, transport.RemoveSubscriber(s1))
	assert.Equal(t, 1, transport.subscribers.Len())

	// Remove second subscriber
	s2.Disconnect()
	require.NoError(t, transport.RemoveSubscriber(s2))
	assert.Zero(t, transport.subscribers.Len())
}

// TestRedisGetSubscribers tests the GetSubscribers method.
func TestRedisGetSubscribers(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Add two subscribers
	s1 := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	require.NoError(t, transport.AddSubscriber(s1))

	s2 := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	require.NoError(t, transport.AddSubscriber(s2))

	// Get subscribers
	lastEventID, subscribers, err := transport.GetSubscribers()
	require.NoError(t, err)

	assert.Equal(t, EarliestLastEventID, lastEventID)
	assert.Len(t, subscribers, 2)
	assert.Contains(t, subscribers, &s1.Subscriber)
	assert.Contains(t, subscribers, &s2.Subscriber)
}

// TestRedisLastEventID tests that the last event ID is correctly persisted and retrieved.
func TestRedisLastEventID(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Dispatch an event with ID "foo"
	require.NoError(t, transport.Dispatch(&Update{
		Event:  Event{ID: "foo"},
		Topics: []string{"test"},
	}))

	// Verify the last event ID was persisted
	lastEventID, _, _ := transport.GetSubscribers()
	assert.Equal(t, "foo", lastEventID)
}

// TestRedisTransportInvalidURL tests that an invalid Redis URL is properly handled.
func TestRedisTransportInvalidURL(t *testing.T) {

	// Try to create a transport with an invalid URL
	_, err := NewRedisTransport(NewSubscriberList(0), zap.NewNop(), "invalid://url", 0, 0)
	require.Error(t, err)
	
	// Verify it returns a TransportError
	var transportErr *TransportError
	assert.ErrorAs(t, err, &transportErr)
}

// TestRedisTransportConnectionFailure tests handling of Redis connection failures.
func TestRedisTransportConnectionFailure(t *testing.T) {

	// Try to connect to a non-existent Redis server
	_, err := NewRedisTransport(NewSubscriberList(0), zap.NewNop(), "redis://localhost:1/0", 0, 0)
	require.Error(t, err)
	
	// Verify it returns a TransportError
	var transportErr *TransportError
	assert.ErrorAs(t, err, &transportErr)
}

// TestRedisTransportConcurrentDispatch tests concurrent dispatching of events.
func TestRedisTransportConcurrentDispatch(t *testing.T) {

	transport := createRedisTransport(t, 0, 0)

	// Create a subscriber
	s := NewLocalSubscriber("", transport.logger, &TopicSelectorStore{})
	s.SetTopics([]string{"https://example.com/test"}, nil)
	require.NoError(t, transport.AddSubscriber(s))

	// Dispatch events concurrently
	const numEvents = 50
	done := make(chan bool)

	go func() {
		for i := 0; i < numEvents; i++ {
			require.NoError(t, transport.Dispatch(&Update{
				Event:  Event{ID: strconv.Itoa(i)},
				Topics: []string{"https://example.com/test"},
			}))
		}
		done <- true
	}()

	// Receive all events
	received := make(map[string]bool)
	go func() {
		for i := 0; i < numEvents; i++ {
			u := <-s.Receive()
			received[u.ID] = true
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Verify all events were received
	assert.Equal(t, numEvents, len(received))
}
