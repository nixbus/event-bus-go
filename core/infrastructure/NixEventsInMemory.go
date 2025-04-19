package infrastructure

import (
	"sync"
	"time"

	"github.com/nixbus/event-bus-go/core/domain"
)

type NixEventsInMemory struct {
	subscribers map[domain.NixEventType][]domain.NixSubscriber
	events      map[domain.NixSubscriberId][]domain.NixEvent
	deadEvents  map[domain.NixSubscriberId][]domain.NixEvent
	retries     map[domain.NixSubscriberId]map[domain.NixEventId]int
	mutex       sync.RWMutex
}

func NewNixEventsInMemory() *NixEventsInMemory {
	return &NixEventsInMemory{
		subscribers: make(map[domain.NixEventType][]domain.NixSubscriber),
		events:      make(map[domain.NixSubscriberId][]domain.NixEvent),
		deadEvents:  make(map[domain.NixSubscriberId][]domain.NixEvent),
		retries:     make(map[domain.NixSubscriberId]map[domain.NixEventId]int),
		mutex:       sync.RWMutex{},
	}
}

func (n *NixEventsInMemory) FindNextEventsFor(subscriber domain.NixSubscriber) ([]domain.NixEvent, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	concurrency := subscriber.Config.Concurrency
	allEvents := n.ensureEventList(subscriber.Id)

	if len(allEvents) == 0 {
		return []domain.NixEvent{}, nil
	}

	var result []domain.NixEvent
	var remainingEvents []domain.NixEvent

	for i, e := range allEvents {
		if i < concurrency {
			retries := n.ensureRetryRecord(subscriber.Id, e.Id)
			if retries < subscriber.Config.MaxRetries {
				result = append(result, e)
			}
		} else {
			remainingEvents = append(remainingEvents, e)
		}
	}

	n.events[subscriber.Id] = remainingEvents
	return result, nil
}

func (n *NixEventsInMemory) FindDeadEventsFor(subscriber domain.NixSubscriber) ([]domain.NixEvent, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	concurrency := subscriber.Config.Concurrency
	allEvents := n.ensureDeadEventList(subscriber.Id)

	if len(allEvents) == 0 {
		return []domain.NixEvent{}, nil
	}

	var result []domain.NixEvent
	var remainingEvents []domain.NixEvent

	for i, e := range allEvents {
		if i < concurrency {
			result = append(result, e)
		} else {
			remainingEvents = append(remainingEvents, e)
		}
	}

	n.deadEvents[subscriber.Id] = remainingEvents
	return result, nil
}

func (n *NixEventsInMemory) GetAllEventsTypesAndPayloads() ([]map[string]any, error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	uniqueEvents := make(map[domain.NixEventId]domain.NixEvent)
	for _, events := range n.events {
		for _, event := range events {
			uniqueEvents[event.Id] = event
		}
	}

	result := make([]map[string]any, 0, len(uniqueEvents))
	for _, event := range uniqueEvents {
		result = append(result, map[string]any{
			"type":    event.Type,
			"payload": event.Payload,
		})
	}

	return result, nil
}

func (n *NixEventsInMemory) GetAllDeadEventsTypesAndPayloads() ([]map[string]any, error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	uniqueEvents := make(map[domain.NixEventId]domain.NixEvent)
	for _, events := range n.deadEvents {
		for _, event := range events {
			uniqueEvents[event.Id] = event
		}
	}

	result := make([]map[string]any, 0, len(uniqueEvents))
	for _, event := range uniqueEvents {
		result = append(result, map[string]any{
			"type":    event.Type,
			"payload": event.Payload,
		})
	}

	return result, nil
}

func (n *NixEventsInMemory) GetSubscribers() ([]domain.NixSubscriber, error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	var result []domain.NixSubscriber
	for _, subscribers := range n.subscribers {
		result = append(result, subscribers...)
	}

	return result, nil
}

func (n *NixEventsInMemory) GetSubscribersByEventType(eventType string) ([]domain.NixSubscriber, error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	eventTypeKey := domain.NixEventType(eventType)
	if _, exists := n.subscribers[eventTypeKey]; !exists {
		n.mutex.RUnlock()
		n.mutex.Lock()
		n.subscribers[eventTypeKey] = []domain.NixSubscriber{}
		n.mutex.Unlock()
		n.mutex.RLock()
	}

	return n.subscribers[eventTypeKey], nil
}

func (n *NixEventsInMemory) Subscribe(eventType string, subscriber domain.NixSubscriber) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	eventTypeKey := domain.NixEventType(eventType)
	if _, exists := n.subscribers[eventTypeKey]; !exists {
		n.subscribers[eventTypeKey] = []domain.NixSubscriber{}
	}

	n.subscribers[eventTypeKey] = append(n.subscribers[eventTypeKey], subscriber)
	return nil
}

func (n *NixEventsInMemory) Unsubscribe(eventType string, subscriberId string) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	eventTypeKey := domain.NixEventType(eventType)
	subscriberIdKey := domain.NixSubscriberId(subscriberId)

	if _, exists := n.subscribers[eventTypeKey]; !exists {
		return nil
	}

	var filteredSubscribers []domain.NixSubscriber
	for _, s := range n.subscribers[eventTypeKey] {
		if s.Id != subscriberIdKey {
			filteredSubscribers = append(filteredSubscribers, s)
		}
	}

	n.subscribers[eventTypeKey] = filteredSubscribers

	delete(n.events, subscriberIdKey)
	delete(n.retries, subscriberIdKey)

	return nil
}

func (n *NixEventsInMemory) UnsubscribeAll() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.subscribers = make(map[domain.NixEventType][]domain.NixSubscriber)
	n.events = make(map[domain.NixSubscriberId][]domain.NixEvent)
	n.retries = make(map[domain.NixSubscriberId]map[domain.NixEventId]int)

	return nil
}

func (n *NixEventsInMemory) MarkAsFailed(event domain.NixEvent, subscriber domain.NixSubscriber) error {
	n.mutex.Lock()
	retries := n.ensureRetryRecord(subscriber.Id, event.Id) + 1
	n.retries[subscriber.Id][event.Id] = retries
	n.mutex.Unlock()

	if retries >= subscriber.Config.MaxRetries {
		n.mutex.Lock()
		delete(n.events, subscriber.Id)
		delete(n.retries, subscriber.Id)
		n.ensureDeadEventList(subscriber.Id)
		n.deadEvents[subscriber.Id] = append(n.deadEvents[subscriber.Id], event)
		n.mutex.Unlock()
		return nil
	}

	time.AfterFunc(time.Duration(subscriber.Config.Timeout)*time.Second, func() {
		n.mutex.Lock()
		defer n.mutex.Unlock()

		if n.retries[subscriber.Id] == nil {
			return
		}

		if currentRetries, exists := n.retries[subscriber.Id][event.Id]; exists && currentRetries < subscriber.Config.MaxRetries {
			n.ensureEventList(subscriber.Id)
			n.events[subscriber.Id] = append(n.events[subscriber.Id], event)
		}
	})

	return nil
}

func (n *NixEventsInMemory) MarkAsFinished(event domain.NixEvent, subscriber domain.NixSubscriber) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.retries[subscriber.Id] != nil {
		delete(n.retries[subscriber.Id], event.Id)
	}

	return nil
}

func (n *NixEventsInMemory) Put(event any) error {
	var e domain.NixEvent

	switch evt := event.(type) {
	case domain.NixNewEvent:
		if evt.Id == "" {
			return domain.EventIdIsRequired
		}
		now := time.Now()
		createdAt := now
		updatedAt := now

		if evt.CreatedAt != nil {
			createdAt = *evt.CreatedAt
		}

		if evt.UpdatedAt != nil {
			updatedAt = *evt.UpdatedAt
		}

		e = domain.NixEvent{
			Id:        evt.Id,
			Type:      evt.Type,
			Payload:   evt.Payload,
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}
	case domain.NixEvent:
		e = evt
	default:
		return nil
	}

	subscribers, err := n.GetSubscribersByEventType(string(e.Type))
	if err != nil {
		return err
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	for _, subscriber := range subscribers {
		n.ensureEventList(subscriber.Id)
		n.events[subscriber.Id] = append(n.events[subscriber.Id], e)
		n.ensureRetryRecord(subscriber.Id, e.Id)
	}

	return nil
}

func (n *NixEventsInMemory) ensureEventList(subscriberId domain.NixSubscriberId) []domain.NixEvent {
	if _, exists := n.events[subscriberId]; !exists {
		n.events[subscriberId] = []domain.NixEvent{}
	}
	return n.events[subscriberId]
}

func (n *NixEventsInMemory) ensureDeadEventList(subscriberId domain.NixSubscriberId) []domain.NixEvent {
	if _, exists := n.deadEvents[subscriberId]; !exists {
		n.deadEvents[subscriberId] = []domain.NixEvent{}
	}
	return n.deadEvents[subscriberId]
}

func (n *NixEventsInMemory) ensureRetryRecord(subscriberId domain.NixSubscriberId, eventId domain.NixEventId) int {
	if _, exists := n.retries[subscriberId]; !exists {
		n.retries[subscriberId] = make(map[domain.NixEventId]int)
	}
	return n.retries[subscriberId][eventId]
}
