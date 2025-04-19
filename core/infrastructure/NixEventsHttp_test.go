package infrastructure_test

import (
	"sort"
	"testing"
	"time"

	"github.com/nixbus/event-bus-go/core/domain"
	"github.com/nixbus/event-bus-go/core/infrastructure"
	"github.com/nixbus/event-bus-go/core/infrastructure/mocks"
	"github.com/stretchr/testify/mock"
)

func TestNixEventsHttp(t *testing.T) {
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
		anyOtherEvent = domain.NixEvent{
			Id:        "any_other_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"key": "value_3"},
			CreatedAt: now,
			UpdatedAt: now,
		}
		oneMoreEvent = domain.NixEvent{
			Id:        "one_more_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"key": "value_4"},
			CreatedAt: now,
			UpdatedAt: now,
		}
		subscriber = domain.NixSubscriber{
			Id: "a_subscriber_id",
			Config: domain.NixSubscriberConfig{
				MaxRetries:  3,
				Timeout:     10,
				Concurrency: 1,
			},
		}
	)

	t.Run("publish events batching the events", func(t *testing.T) {
		t.Parallel()
		mockHttpClient := new(mocks.NixBusHttpClientInterface)
		mockHttpClient.On("PublishEvents", mock.MatchedBy(func(events []struct {
			Type    string
			Payload map[string]any
		},
		) bool {
			return len(events) > 0
		})).Return(nil)

		nixEventsHttp := infrastructure.NewNixEventsHttp(infrastructure.NixEventsHttpDeps{
			Client: mockHttpClient,
		})

		// Publish events concurrently
		go nixEventsHttp.Put(event)
		go nixEventsHttp.Put(anotherEvent)
		go nixEventsHttp.Put(anyOtherEvent)
		go nixEventsHttp.Put(oneMoreEvent)

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		// Verify calls
		calls := mockHttpClient.Calls
		var allEvents []struct {
			Type    string
			Payload map[string]any
		}

		for _, call := range calls {
			if call.Method == "PublishEvents" {
				events := call.Arguments.Get(0).([]struct {
					Type    string
					Payload map[string]any
				})
				allEvents = append(allEvents, events...)
			}
		}

		// Sort events by payload value for consistent comparison
		sort.Slice(allEvents, func(i, j int) bool {
			return allEvents[i].Payload["key"].(string) < allEvents[j].Payload["key"].(string)
		})

		expectedEvents := []struct {
			Type    string
			Payload map[string]any
		}{
			{Type: "an_event_type", Payload: map[string]any{"key": "value_1"}},
			{Type: "an_event_type", Payload: map[string]any{"key": "value_2"}},
			{Type: "an_event_type", Payload: map[string]any{"key": "value_3"}},
			{Type: "an_event_type", Payload: map[string]any{"key": "value_4"}},
		}

		if len(allEvents) != len(expectedEvents) {
			t.Errorf("Expected %d events, got %d", len(expectedEvents), len(allEvents))
		}

		for i, e := range expectedEvents {
			if allEvents[i].Type != e.Type || allEvents[i].Payload["key"] != e.Payload["key"] {
				t.Errorf("Event mismatch at index %d. Expected %v, got %v", i, e, allEvents[i])
			}
		}
	})

	t.Run("mark events as finished batching the events", func(t *testing.T) {
		t.Parallel()
		mockHttpClient := new(mocks.NixBusHttpClientInterface)
		mockHttpClient.On("MarkEventsAsFinished", mock.MatchedBy(func(events []struct {
			Id           string
			SubscriberId string
		},
		) bool {
			return len(events) > 0
		})).Return(nil)

		nixEventsHttp := infrastructure.NewNixEventsHttp(infrastructure.NixEventsHttpDeps{
			Client: mockHttpClient,
		})

		// Mark events as finished concurrently
		go nixEventsHttp.MarkAsFinished(event, subscriber)
		go nixEventsHttp.MarkAsFinished(anotherEvent, subscriber)
		go nixEventsHttp.MarkAsFinished(anyOtherEvent, subscriber)
		go nixEventsHttp.MarkAsFinished(oneMoreEvent, subscriber)

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		// Verify calls
		calls := mockHttpClient.Calls
		var allEvents []struct {
			Id           string
			SubscriberId string
		}

		for _, call := range calls {
			if call.Method == "MarkEventsAsFinished" {
				events := call.Arguments.Get(0).([]struct {
					Id           string
					SubscriberId string
				})
				allEvents = append(allEvents, events...)
			}
		}

		// Sort events by ID for consistent comparison
		sort.Slice(allEvents, func(i, j int) bool {
			return allEvents[i].Id < allEvents[j].Id
		})

		expectedEvents := []struct {
			Id           string
			SubscriberId string
		}{
			{Id: "an_event_id", SubscriberId: "a_subscriber_id"},
			{Id: "another_event_id", SubscriberId: "a_subscriber_id"},
			{Id: "any_other_event_id", SubscriberId: "a_subscriber_id"},
			{Id: "one_more_event_id", SubscriberId: "a_subscriber_id"},
		}

		if len(allEvents) != len(expectedEvents) {
			t.Errorf("Expected %d events, got %d", len(expectedEvents), len(allEvents))
		}

		for i, e := range expectedEvents {
			if allEvents[i].Id != e.Id || allEvents[i].SubscriberId != e.SubscriberId {
				t.Errorf("Event mismatch at index %d. Expected %v, got %v", i, e, allEvents[i])
			}
		}
	})

	t.Run("mark events as failed batching the events", func(t *testing.T) {
		t.Parallel()
		mockHttpClient := new(mocks.NixBusHttpClientInterface)
		mockHttpClient.On("MarkEventsAsFailed", mock.MatchedBy(func(events []struct {
			Id           string
			SubscriberId string
		},
		) bool {
			return len(events) > 0
		})).Return(nil)

		nixEventsHttp := infrastructure.NewNixEventsHttp(infrastructure.NixEventsHttpDeps{
			Client: mockHttpClient,
		})

		// Mark events as failed concurrently
		go nixEventsHttp.MarkAsFailed(event, subscriber)
		go nixEventsHttp.MarkAsFailed(anotherEvent, subscriber)
		go nixEventsHttp.MarkAsFailed(anyOtherEvent, subscriber)
		go nixEventsHttp.MarkAsFailed(oneMoreEvent, subscriber)

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		// Verify calls
		calls := mockHttpClient.Calls
		var allEvents []struct {
			Id           string
			SubscriberId string
		}

		for _, call := range calls {
			if call.Method == "MarkEventsAsFailed" {
				events := call.Arguments.Get(0).([]struct {
					Id           string
					SubscriberId string
				})
				allEvents = append(allEvents, events...)
			}
		}

		// Sort events by ID for consistent comparison
		sort.Slice(allEvents, func(i, j int) bool {
			return allEvents[i].Id < allEvents[j].Id
		})

		expectedEvents := []struct {
			Id           string
			SubscriberId string
		}{
			{Id: "an_event_id", SubscriberId: "a_subscriber_id"},
			{Id: "another_event_id", SubscriberId: "a_subscriber_id"},
			{Id: "any_other_event_id", SubscriberId: "a_subscriber_id"},
			{Id: "one_more_event_id", SubscriberId: "a_subscriber_id"},
		}

		if len(allEvents) != len(expectedEvents) {
			t.Errorf("Expected %d events, got %d", len(expectedEvents), len(allEvents))
		}

		for i, e := range expectedEvents {
			if allEvents[i].Id != e.Id || allEvents[i].SubscriberId != e.SubscriberId {
				t.Errorf("Event mismatch at index %d. Expected %v, got %v", i, e, allEvents[i])
			}
		}
	})
}
