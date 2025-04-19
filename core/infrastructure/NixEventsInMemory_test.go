package infrastructure_test

import (
	"testing"
	"time"

	"github.com/nixbus/event-bus-go/core/domain"
	"github.com/nixbus/event-bus-go/core/infrastructure"
	"github.com/stretchr/testify/assert"
)

func TestNixEventsInMemory(t *testing.T) {
	var (
		now   = time.Now()
		event = domain.NixEvent{
			Id:        "an_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"key": "value_1"},
			CreatedAt: now,
			UpdatedAt: now,
		}
		anotherEvent = domain.NixEvent{
			Id:        "another_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"key": "value_2"},
			CreatedAt: now,
			UpdatedAt: now,
		}
		subscriber = domain.NixSubscriber{
			Id: "a_subscriber_id",
			Config: domain.NixSubscriberConfig{
				MaxRetries:  3,
				Timeout:     1, // Short timeout for testing
				Concurrency: 1,
			},
		}
	)

	t.Run("put and find events", func(t *testing.T) {
		t.Parallel()
		events := infrastructure.NewNixEventsInMemory()

		// Subscribe to events first
		err := events.Subscribe("an_event_type", subscriber)
		assert.NoError(t, err)

		// Put events
		err = events.Put(event)
		assert.NoError(t, err)
		err = events.Put(anotherEvent)
		assert.NoError(t, err)

		// Find events for subscriber
		foundEvents, err := events.FindNextEventsFor(subscriber)
		assert.NoError(t, err)
		assert.Len(t, foundEvents, 1) // Due to concurrency limit of 1
		assert.Equal(t, event.Id, foundEvents[0].Id)

		// Find remaining events
		foundEvents, err = events.FindNextEventsFor(subscriber)
		assert.NoError(t, err)
		assert.Len(t, foundEvents, 1)
		assert.Equal(t, anotherEvent.Id, foundEvents[0].Id)
	})

	t.Run("mark events as failed and find dead events", func(t *testing.T) {
		t.Parallel()
		events := infrastructure.NewNixEventsInMemory()

		// Subscribe to events first
		err := events.Subscribe("an_event_type", subscriber)
		assert.NoError(t, err)

		// Put event
		err = events.Put(event)
		assert.NoError(t, err)

		// Find and mark as failed multiple times
		for range subscriber.Config.MaxRetries {
			foundEvents, err := events.FindNextEventsFor(subscriber)
			assert.NoError(t, err)
			assert.Len(t, foundEvents, 1)
			assert.Equal(t, event.Id, foundEvents[0].Id)

			err = events.MarkAsFailed(foundEvents[0], subscriber)
			assert.NoError(t, err)

			// Wait for the retry timeout
			time.Sleep(time.Duration(subscriber.Config.Timeout+1) * time.Second)
		}

		// Find dead events
		deadEvents, err := events.FindDeadEventsFor(subscriber)
		assert.NoError(t, err)
		assert.Len(t, deadEvents, 1)
		assert.Equal(t, event.Id, deadEvents[0].Id)
	})

	t.Run("mark events as finished", func(t *testing.T) {
		t.Parallel()
		events := infrastructure.NewNixEventsInMemory()

		// Subscribe to events first
		err := events.Subscribe("an_event_type", subscriber)
		assert.NoError(t, err)

		// Put event
		err = events.Put(event)
		assert.NoError(t, err)

		// Find and mark as finished
		foundEvents, err := events.FindNextEventsFor(subscriber)
		assert.NoError(t, err)
		assert.Len(t, foundEvents, 1)
		assert.Equal(t, event.Id, foundEvents[0].Id)

		err = events.MarkAsFinished(foundEvents[0], subscriber)
		assert.NoError(t, err)

		// Verify event is removed
		foundEvents, err = events.FindNextEventsFor(subscriber)
		assert.NoError(t, err)
		assert.Empty(t, foundEvents)
	})

	t.Run("get all events types and payloads", func(t *testing.T) {
		t.Parallel()
		events := infrastructure.NewNixEventsInMemory()

		// Subscribe to events first
		err := events.Subscribe("an_event_type", subscriber)
		assert.NoError(t, err)

		// Put events
		err = events.Put(event)
		assert.NoError(t, err)
		err = events.Put(anotherEvent)
		assert.NoError(t, err)

		// Get all events
		allEvents, err := events.GetAllEventsTypesAndPayloads()
		assert.NoError(t, err)
		assert.Len(t, allEvents, 2)

		// Verify event types and payloads
		eventTypes := make(map[string]bool)
		for _, e := range allEvents {
			// Convert domain.NixEventType to string
			eventTypeStr := string(e["type"].(domain.NixEventType))
			eventTypes[eventTypeStr] = true
			assert.Contains(t, []string{"value_1", "value_2"}, e["payload"].(map[string]any)["key"].(string))
		}
		assert.True(t, eventTypes["an_event_type"])
	})

	t.Run("subscribe and unsubscribe", func(t *testing.T) {
		t.Parallel()
		events := infrastructure.NewNixEventsInMemory()

		// Subscribe
		err := events.Subscribe("an_event_type", subscriber)
		assert.NoError(t, err)

		// Get subscribers
		subscribers, err := events.GetSubscribers()
		assert.NoError(t, err)
		assert.Len(t, subscribers, 1)
		assert.Equal(t, subscriber.Id, subscribers[0].Id)

		// Get subscribers by event type
		subscribers, err = events.GetSubscribersByEventType("an_event_type")
		assert.NoError(t, err)
		assert.Len(t, subscribers, 1)
		assert.Equal(t, subscriber.Id, subscribers[0].Id)

		// Unsubscribe
		err = events.Unsubscribe("an_event_type", string(subscriber.Id))
		assert.NoError(t, err)

		// Verify unsubscribe
		subscribers, err = events.GetSubscribers()
		assert.NoError(t, err)
		assert.Empty(t, subscribers)
	})

	t.Run("unsubscribe all", func(t *testing.T) {
		t.Parallel()
		events := infrastructure.NewNixEventsInMemory()

		// Subscribe multiple times
		err := events.Subscribe("an_event_type", subscriber)
		assert.NoError(t, err)
		err = events.Subscribe("another_event_type", subscriber)
		assert.NoError(t, err)

		// Get subscribers
		subscribers, err := events.GetSubscribers()
		assert.NoError(t, err)
		assert.Len(t, subscribers, 2)

		// Unsubscribe all
		err = events.UnsubscribeAll()
		assert.NoError(t, err)

		// Verify unsubscribe all
		subscribers, err = events.GetSubscribers()
		assert.NoError(t, err)
		assert.Empty(t, subscribers)
	})

	t.Run("mark events as failed and find dead events", func(t *testing.T) {
		t.Parallel()
		// Given
		events := infrastructure.NewNixEventsInMemory()
		subscriber := domain.NixSubscriber{
			Id: "subscriber-1",
			Config: domain.NixSubscriberConfig{
				MaxRetries:  1,
				Timeout:     1,
				Concurrency: 1,
			},
		}
		event := domain.NixEvent{
			Id:      "event-1",
			Type:    "event-type-1",
			Payload: map[string]any{"data": "payload-1"},
		}

		// When
		events.Subscribe("event-type-1", subscriber)
		events.Put(event)
		nextEvents, _ := events.FindNextEventsFor(subscriber)
		events.MarkAsFailed(nextEvents[0], subscriber)

		// Wait for the retry timeout
		time.Sleep(2 * time.Second)

		// Then
		deadEvents, _ := events.FindDeadEventsFor(subscriber)
		assert.Equal(t, 1, len(deadEvents))
		assert.Equal(t, event.Id, deadEvents[0].Id)
	})
}
