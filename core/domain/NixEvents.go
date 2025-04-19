package domain

type NixEvents interface {
	FindNextEventsFor(subscriber NixSubscriber) ([]NixEvent, error)

	FindDeadEventsFor(subscriber NixSubscriber) ([]NixEvent, error)

	GetSubscribers() ([]NixSubscriber, error)

	GetSubscribersByEventType(eventType string) ([]NixSubscriber, error)

	MarkAsFailed(event NixEvent, subscriber NixSubscriber) error

	MarkAsFinished(event NixEvent, subscriber NixSubscriber) error

	Put(event any) error

	Subscribe(eventType string, subscriber NixSubscriber) error

	Unsubscribe(eventType string, subscriberId string) error

	UnsubscribeAll() error
}
