package domain_test

import (
	"sync"
	"testing"
	"time"

	"github.com/nixbus/event-bus-go/core/domain"
	"github.com/nixbus/event-bus-go/core/infrastructure"
	"github.com/stretchr/testify/assert"
)

func newTestLogger() domain.Logger {
	return infrastructure.NewLogger(infrastructure.LogLevel{
		Level: "debug",
	})
}

func newTestEventBus() (*infrastructure.NixEventsInMemory, *domain.NixEventBus) {
	events := infrastructure.NewNixEventsInMemory()
	bus := domain.NewNixEventBus(domain.EventBusDeps{
		Events: events,
		Logger: newTestLogger(),
	})
	return events, bus
}

func TestNixEventBus(t *testing.T) {
	t.Run("subscribe to events", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()

		err := bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error { return nil },
		})
		assert.NoError(t, err)

		err = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error { return nil },
		})
		assert.NoError(t, err)

		verifySubscribers(t, events, []domain.NixSubscriberId{"a_subscriber_id", "another_subscriber_id"})
	})

	t.Run("unsubscribe to events", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()

		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error { return nil },
		})
		_ = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error { return nil },
		})

		_ = bus.Unsubscribe("an_event_type", "a_subscriber_id")

		verifySubscribers(t, events, []domain.NixSubscriberId{"another_subscriber_id"})
	})

	t.Run("unsubscribe all events", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()

		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error { return nil },
		})
		_ = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error { return nil },
		})

		_ = bus.UnsubscribeAll()

		verifySubscribers(t, events, []domain.NixSubscriberId{})
	})

	t.Run("publish events", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()

		localActions := make(map[string][]domain.NixEvent)
		var mu sync.Mutex

		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["a_subscriber_id"] = append(localActions["a_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})
		_ = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["another_subscriber_id"] = append(localActions["another_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})

		createdAt := time.Date(2024, 5, 5, 14, 24, 5, 1884*1e6, time.UTC)
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "an_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"hello": "world"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "another_event_id",
			Type:      "another_event_type",
			Payload:   map[string]any{"hello": "world 2"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})

		verifyEvents(t, events, []string{"an_event_type", "another_event_type"})

		go bus.Run()
		time.Sleep(2 * time.Second)
		bus.Stop()

		mu.Lock()
		defer mu.Unlock()
		assert.NotEmpty(t, localActions["a_subscriber_id"])
		assert.NotEmpty(t, localActions["another_subscriber_id"])
	})

	t.Run("consume all events and dead events", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()
		localActions := make(map[string][]domain.NixEvent)
		deadActions := make(map[string][]domain.NixEvent)
		var mu sync.Mutex

		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 1, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["a_subscriber_id"] = append(localActions["a_subscriber_id"], event)
				mu.Unlock()
				return assert.AnError
			},
		})
		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 1, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["another_subscriber_id"] = append(localActions["another_subscriber_id"], event)
				mu.Unlock()
				return assert.AnError
			},
			DeadAction: func(event domain.NixEvent) error {
				mu.Lock()
				deadActions["another_subscriber_id"] = append(deadActions["another_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})
		_ = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "any_other_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 1, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["any_other_subscriber_id"] = append(localActions["any_other_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})

		createdAt := time.Date(2024, 5, 5, 14, 24, 5, 184*1e6, time.UTC)
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "an_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"hello": "world"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "another_event_id",
			Type:      "another_event_type",
			Payload:   map[string]any{"hello": "world 2"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})

		verifyEvents(t, events, []string{"an_event_type", "another_event_type"})

		go bus.Run()
		time.Sleep(8 * time.Second)
		bus.Stop()

		mu.Lock()
		defer mu.Unlock()
		assert.NotEmpty(t, localActions["a_subscriber_id"])
		assert.NotEmpty(t, localActions["another_subscriber_id"])
		assert.NotEmpty(t, deadActions["another_subscriber_id"])
		assert.NotEmpty(t, localActions["any_other_subscriber_id"])
	})

	t.Run("consume all events and unsubscribe subscriber", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()
		localActions := make(map[string][]domain.NixEvent)
		var mu sync.Mutex

		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["a_subscriber_id"] = append(localActions["a_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})
		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["another_subscriber_id"] = append(localActions["another_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})
		_ = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "any_other_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["any_other_subscriber_id"] = append(localActions["any_other_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})

		createdAt := time.Date(2024, 5, 5, 14, 24, 5, 184*1e6, time.UTC)
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "an_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"hello": "world"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "another_event_id",
			Type:      "another_event_type",
			Payload:   map[string]any{"hello": "world 2"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})

		verifyEvents(t, events, []string{"an_event_type", "another_event_type"})

		go bus.Run()
		time.Sleep(2 * time.Second)
		bus.Stop()

		_ = bus.Unsubscribe("an_event_type", "a_subscriber_id")

		mu.Lock()
		defer mu.Unlock()
		assert.NotEmpty(t, localActions["another_subscriber_id"])
		assert.NotEmpty(t, localActions["any_other_subscriber_id"])
	})

	t.Run("consume all events and unsubscribe all", func(t *testing.T) {
		t.Parallel()
		events, bus := newTestEventBus()
		localActions := make(map[string][]domain.NixEvent)
		var mu sync.Mutex

		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "a_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["a_subscriber_id"] = append(localActions["a_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})
		_ = bus.Subscribe("an_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "another_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["another_subscriber_id"] = append(localActions["another_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})
		_ = bus.Subscribe("another_event_type", domain.SubscriberWithAction{
			NixSubscriber: domain.NixSubscriber{
				Id:     "any_other_subscriber_id",
				Config: domain.NixSubscriberConfig{MaxRetries: 3, Timeout: 10, Concurrency: 5},
			},
			Action: func(event domain.NixEvent) error {
				mu.Lock()
				localActions["any_other_subscriber_id"] = append(localActions["any_other_subscriber_id"], event)
				mu.Unlock()
				return nil
			},
		})

		createdAt := time.Date(2024, 5, 5, 14, 24, 5, 184*1e6, time.UTC)
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "an_event_id",
			Type:      "an_event_type",
			Payload:   map[string]any{"hello": "world"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})
		_ = bus.Publish(domain.NixNewEvent{
			Id:        "another_event_id",
			Type:      "another_event_type",
			Payload:   map[string]any{"hello": "world 2"},
			CreatedAt: &createdAt,
			UpdatedAt: &createdAt,
		})

		verifyEvents(t, events, []string{"an_event_type", "another_event_type"})

		go bus.Run()
		time.Sleep(2 * time.Second)
		bus.Stop()

		_ = bus.UnsubscribeAll()

		mu.Lock()
		defer mu.Unlock()
		assert.NotEmpty(t, localActions["a_subscriber_id"])
		assert.NotEmpty(t, localActions["another_subscriber_id"])
		assert.NotEmpty(t, localActions["any_other_subscriber_id"])
	})
}

// --- Helpers ---

func verifySubscribers(t *testing.T, events *infrastructure.NixEventsInMemory, expected []domain.NixSubscriberId) {
	subs, err := events.GetSubscribers()
	assert.NoError(t, err)
	var ids []domain.NixSubscriberId
	for _, s := range subs {
		ids = append(ids, s.Id)
	}
	assert.ElementsMatch(t, expected, ids)
}

func verifyEvents(t *testing.T, events *infrastructure.NixEventsInMemory, expectedTypes []string) {
	allEvents, err := events.GetAllEventsTypesAndPayloads()
	assert.NoError(t, err)
	var types []string
	for _, e := range allEvents {
		types = append(types, string(e["type"].(domain.NixEventType)))
	}
	assert.ElementsMatch(t, expectedTypes, types)
}
