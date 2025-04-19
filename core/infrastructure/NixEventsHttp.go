package infrastructure

import (
	"sync"

	"github.com/nixbus/event-bus-go/core/domain"
)

type NixEventsHttpDeps struct {
	Client NixBusHttpClientInterface
}

type NixEventsHttp struct {
	deps             NixEventsHttpDeps
	subscribers      map[domain.NixSubscriberId]domain.NixSubscriber
	markedAsFinished []struct{ Id, SubscriberId string }
	markedAsFailed   []struct{ Id, SubscriberId string }
	eventsToPublish  []struct {
		Type    string
		Payload map[string]any
	}
	isPublishingEvents        bool
	isMarkingEventsAsFailed   bool
	isMarkingEventsAsFinished bool
	mutex                     sync.RWMutex
}

func NewNixEventsHttp(deps NixEventsHttpDeps) *NixEventsHttp {
	return &NixEventsHttp{
		deps:             deps,
		subscribers:      make(map[domain.NixSubscriberId]domain.NixSubscriber),
		markedAsFinished: []struct{ Id, SubscriberId string }{},
		markedAsFailed:   []struct{ Id, SubscriberId string }{},
		eventsToPublish: []struct {
			Type    string
			Payload map[string]any
		}{},
		isPublishingEvents:        false,
		isMarkingEventsAsFailed:   false,
		isMarkingEventsAsFinished: false,
		mutex:                     sync.RWMutex{},
	}
}

func (n *NixEventsHttp) FindNextEventsFor(subscriber domain.NixSubscriber) ([]domain.NixEvent, error) {
	response, err := n.deps.Client.FindNextEvents(string(subscriber.Id))
	if err != nil {
		return []domain.NixEvent{}, err
	}

	events := make([]domain.NixEvent, len(response.Events))
	for i, event := range response.Events {
		events[i] = n.serializeEvent(event)
	}

	return events, nil
}

func (n *NixEventsHttp) FindDeadEventsFor(subscriber domain.NixSubscriber) ([]domain.NixEvent, error) {
	response, err := n.deps.Client.FindDeadEvents(string(subscriber.Id))
	if err != nil {
		return []domain.NixEvent{}, err
	}

	events := make([]domain.NixEvent, len(response.Events))
	for i, event := range response.Events {
		events[i] = n.serializeEvent(event)
	}

	return events, nil
}

func (n *NixEventsHttp) GetSubscribers() ([]domain.NixSubscriber, error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	subscribers := make([]domain.NixSubscriber, 0, len(n.subscribers))
	for _, subscriber := range n.subscribers {
		subscribers = append(subscribers, subscriber)
	}

	return subscribers, nil
}

func (n *NixEventsHttp) GetSubscribersByEventType(eventType string) ([]domain.NixSubscriber, error) {
	return []domain.NixSubscriber{}, nil
}

func (n *NixEventsHttp) Subscribe(eventType string, subscriber domain.NixSubscriber) error {
	err := n.deps.Client.PutSubscriber(
		string(subscriber.Id),
		eventType,
		struct {
			MaxRetries  int
			Timeout     int
			Concurrency int
		}{
			MaxRetries:  subscriber.Config.MaxRetries,
			Timeout:     subscriber.Config.Timeout,
			Concurrency: subscriber.Config.Concurrency,
		},
	)
	if err != nil {
		return err
	}

	n.mutex.Lock()
	n.subscribers[subscriber.Id] = subscriber
	n.mutex.Unlock()

	return nil
}

func (n *NixEventsHttp) Unsubscribe(eventType string, subscriberId string) error {
	return n.deps.Client.RemoveSubscriber(eventType, subscriberId)
}

func (n *NixEventsHttp) UnsubscribeAll() error {
	return n.deps.Client.RemoveAllSubscribers()
}

func (n *NixEventsHttp) MarkAsFailed(event domain.NixEvent, subscriber domain.NixSubscriber) error {
	n.mutex.Lock()
	n.markedAsFailed = append(n.markedAsFailed, struct{ Id, SubscriberId string }{
		Id:           string(event.Id),
		SubscriberId: string(subscriber.Id),
	})
	isMarkingEventsAsFailed := n.isMarkingEventsAsFailed
	n.mutex.Unlock()

	if isMarkingEventsAsFailed {
		return nil
	}

	n.mutex.Lock()
	n.isMarkingEventsAsFailed = true
	n.mutex.Unlock()

	for {
		n.mutex.Lock()
		if len(n.markedAsFailed) == 0 {
			n.isMarkingEventsAsFailed = false
			n.mutex.Unlock()
			return nil
		}

		events := n.markedAsFailed
		n.markedAsFailed = []struct{ Id, SubscriberId string }{}
		n.mutex.Unlock()

		err := n.deps.Client.MarkEventsAsFailed(events)
		if err != nil {
			n.mutex.Lock()
			n.isMarkingEventsAsFailed = false
			n.mutex.Unlock()
			return err
		}
	}
}

func (n *NixEventsHttp) MarkAsFinished(event domain.NixEvent, subscriber domain.NixSubscriber) error {
	n.mutex.Lock()
	n.markedAsFinished = append(n.markedAsFinished, struct{ Id, SubscriberId string }{
		Id:           string(event.Id),
		SubscriberId: string(subscriber.Id),
	})
	isMarkingEventsAsFinished := n.isMarkingEventsAsFinished
	n.mutex.Unlock()

	if isMarkingEventsAsFinished {
		return nil
	}

	n.mutex.Lock()
	n.isMarkingEventsAsFinished = true
	n.mutex.Unlock()

	for {
		n.mutex.Lock()
		if len(n.markedAsFinished) == 0 {
			n.isMarkingEventsAsFinished = false
			n.mutex.Unlock()
			return nil
		}

		events := n.markedAsFinished
		n.markedAsFinished = []struct{ Id, SubscriberId string }{}
		n.mutex.Unlock()

		err := n.deps.Client.MarkEventsAsFinished(events)
		if err != nil {
			n.mutex.Lock()
			n.isMarkingEventsAsFinished = false
			n.mutex.Unlock()
			return err
		}
	}
}

func (n *NixEventsHttp) Put(event any) error {
	var e struct {
		Type    string
		Payload map[string]any
	}

	switch evt := event.(type) {
	case domain.NixNewEvent:
		e.Type = string(evt.Type)
		e.Payload = evt.Payload
	case domain.NixEvent:
		e.Type = string(evt.Type)
		e.Payload = evt.Payload
	default:
		return nil
	}

	n.mutex.Lock()
	n.eventsToPublish = append(n.eventsToPublish, e)
	isPublishingEvents := n.isPublishingEvents
	n.mutex.Unlock()

	if isPublishingEvents {
		return nil
	}

	n.mutex.Lock()
	n.isPublishingEvents = true
	n.mutex.Unlock()

	for {
		n.mutex.Lock()
		if len(n.eventsToPublish) == 0 {
			n.isPublishingEvents = false
			n.mutex.Unlock()
			return nil
		}

		events := n.eventsToPublish
		n.eventsToPublish = []struct {
			Type    string
			Payload map[string]any
		}{}
		n.mutex.Unlock()

		err := n.deps.Client.PublishEvents(events)
		if err != nil {
			n.mutex.Lock()
			n.isPublishingEvents = false
			n.mutex.Unlock()
			return err
		}
	}
}

func (n *NixEventsHttp) serializeEvent(event FindEventResponse) domain.NixEvent {
	return domain.NixEvent{
		Id:        domain.NixEventId(event.Id),
		Type:      domain.NixEventType(event.Type),
		Payload:   event.Payload,
		CreatedAt: event.CreatedAt,
		UpdatedAt: event.UpdatedAt,
	}
}
