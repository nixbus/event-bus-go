package domain

type NixSubscriberId string

type NixSubscriberConfig struct {
	MaxRetries  int `json:"maxRetries"`
	Timeout     int `json:"timeout"`
	Concurrency int `json:"concurrency"`
}

type NixSubscriber struct {
	Id     NixSubscriberId     `json:"id"`
	Config NixSubscriberConfig `json:"config"`
}

type NixSubscriberAction func(event NixEvent) error
