//go:generate mockery --name=NixBusHttpClientInterface --dir=. --output=./mocks
package infrastructure

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	crypto "github.com/nixbus/crypto-go"
	"github.com/nixbus/event-bus-go/core/domain"
)

type NixBusHttpClientInterface interface {
	FindNextEvents(subscriberId string) (*FindEventsResponse, error)
	FindDeadEvents(subscriberId string) (*FindEventsResponse, error)
	GetSubscribers() (*SubscribersResponse, error)
	MarkEventsAsFailed(events []struct {
		Id           string
		SubscriberId string
	}) error
	MarkEventsAsFinished(events []struct {
		Id           string
		SubscriberId string
	}) error
	PublishEvents(events []struct {
		Type    string
		Payload map[string]any
	}) error
	PutSubscriber(subscriberId string, eventType string, config struct {
		MaxRetries  int
		Timeout     int
		Concurrency int
	}) error
	RemoveSubscriber(eventType string, subscriberId string) error
	RemoveAllSubscribers() error
}

type EventsResponse struct {
	Events []EventResponse `json:"events"`
}

type EventResponse struct {
	Id        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type FindEventsResponse struct {
	Events []FindEventResponse `json:"events"`
}

type FindEventResponse struct {
	Id        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type FindDeadEventResponse struct {
	Id        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type SubscribersResponse struct {
	Subscribers []SubscriberResponse `json:"subscribers"`
}

type SubscriberResponse struct {
	Id        string `json:"id"`
	EventType string `json:"event_type"`
	Config    struct {
		MaxRetries  int `json:"max_retries"`
		Timeout     int `json:"timeout"`
		Concurrency int `json:"concurrency"`
	} `json:"config"`
}

type FindNextEventsRequest struct {
	SubscriberId string `json:"subscriber_id"`
}

type FindDeadEventsRequest struct {
	SubscriberId string `json:"subscriber_id"`
}

type MarkEventsAsFailedRequest struct {
	Events []struct {
		Id           string `json:"id"`
		SubscriberId string `json:"subscriber_id"`
	} `json:"events"`
}

type MarkEventsAsFinishedRequest struct {
	Events []struct {
		Id           string `json:"id"`
		SubscriberId string `json:"subscriber_id"`
	} `json:"events"`
}

type PublishEventsRequest struct {
	Events []struct {
		Type    string `json:"type"`
		Payload string `json:"payload"`
	} `json:"events"`
}

type PutSubscriberRequest struct {
	SubscriberId string `json:"subscriber_id"`
	EventType    string `json:"event_type"`
	Config       struct {
		MaxRetries  int `json:"max_retries"`
		Timeout     int `json:"timeout"`
		Concurrency int `json:"concurrency"`
	} `json:"config"`
}

type RemoveSubscriberRequest struct {
	EventType    string `json:"event_type"`
	SubscriberId string `json:"subscriber_id"`
}

type NixBusHttpClientDeps struct {
	Crypto *crypto.NixBusCrypto
	Logger Logger
}

type NixBusHttpClientOptions struct {
	Token   string
	BaseUrl string
}

type NixBusHttpClient struct {
	deps                  NixBusHttpClientDeps
	opts                  NixBusHttpClientOptions
	baseUrl               string
	findNextEventsTimeout map[domain.NixSubscriberId]int
	mutex                 sync.RWMutex
	httpClient            *http.Client
}

func NewNixBusHttpClient(deps NixBusHttpClientDeps, opts NixBusHttpClientOptions) *NixBusHttpClient {
	if opts.Token == "" {
		panic("[NixBusHttpClient] token is required. Get your free token at https://nixbus.com")
	}

	baseUrl := opts.BaseUrl
	if baseUrl == "" {
		baseUrl = "https://nixbus.com/api/v1"
	}

	return &NixBusHttpClient{
		deps:                  deps,
		opts:                  opts,
		baseUrl:               baseUrl,
		findNextEventsTimeout: make(map[domain.NixSubscriberId]int),
		mutex:                 sync.RWMutex{},
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *NixBusHttpClient) FindNextEvents(subscriberId string) (*FindEventsResponse, error) {
	c.mutex.RLock()
	timeout, exists := c.findNextEventsTimeout[domain.NixSubscriberId(subscriberId)]
	c.mutex.RUnlock()

	if exists && timeout > 0 {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
	}

	body := FindNextEventsRequest{
		SubscriberId: subscriberId,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/find_next_events", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data EventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	c.mutex.Lock()
	if len(data.Events) == 0 {
		if _, exists := c.findNextEventsTimeout[domain.NixSubscriberId(subscriberId)]; !exists {
			c.findNextEventsTimeout[domain.NixSubscriberId(subscriberId)] = 0
		}

		if c.findNextEventsTimeout[domain.NixSubscriberId(subscriberId)] < 30000 {
			c.findNextEventsTimeout[domain.NixSubscriberId(subscriberId)] += 1000
		}
	} else {
		c.findNextEventsTimeout = make(map[domain.NixSubscriberId]int)
	}
	c.mutex.Unlock()

	events := make([]FindEventResponse, 0, len(data.Events))
	for _, event := range data.Events {
		deserializedEvent, err := c.deserialize(subscriberId, event)
		if err != nil {
			c.deps.Logger.Error("NixBusHttpClient", "deserialize", map[string]any{
				"error": err,
			})
			continue
		}
		if deserializedEvent != nil {
			events = append(events, *deserializedEvent)
		}
	}

	return &FindEventsResponse{
		Events: events,
	}, nil
}

func (c *NixBusHttpClient) FindDeadEvents(subscriberId string) (*FindEventsResponse, error) {
	body := FindDeadEventsRequest{
		SubscriberId: subscriberId,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/find_dead_events", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data EventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	events := make([]FindEventResponse, 0, len(data.Events))
	for _, event := range data.Events {
		deserializedEvent, err := c.deserialize(subscriberId, event)
		if err != nil {
			c.deps.Logger.Error("NixBusHttpClient", "deserialize", map[string]any{
				"error": err,
			})
			continue
		}
		if deserializedEvent != nil {
			events = append(events, *deserializedEvent)
		}
	}

	return &FindEventsResponse{
		Events: events,
	}, nil
}

func (c *NixBusHttpClient) GetSubscribers() (*SubscribersResponse, error) {
	resp, err := c.fetchJSON(fmt.Sprintf("%s/get_subscribers", c.baseUrl), "POST", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data SubscribersResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	return &data, nil
}

func (c *NixBusHttpClient) MarkEventsAsFailed(events []struct {
	Id           string
	SubscriberId string
},
) error {
	requestEvents := make([]struct {
		Id           string `json:"id"`
		SubscriberId string `json:"subscriber_id"`
	}, len(events))

	for i, event := range events {
		requestEvents[i].Id = event.Id
		requestEvents[i].SubscriberId = event.SubscriberId
	}

	body := MarkEventsAsFailedRequest{
		Events: requestEvents,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/mark_events_as_failed", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *NixBusHttpClient) MarkEventsAsFinished(events []struct {
	Id           string
	SubscriberId string
},
) error {
	requestEvents := make([]struct {
		Id           string `json:"id"`
		SubscriberId string `json:"subscriber_id"`
	}, len(events))

	for i, event := range events {
		requestEvents[i].Id = event.Id
		requestEvents[i].SubscriberId = event.SubscriberId
	}

	body := MarkEventsAsFinishedRequest{
		Events: requestEvents,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/mark_events_as_finished", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *NixBusHttpClient) PublishEvents(events []struct {
	Type    string
	Payload map[string]any
},
) error {
	requestEvents := make([]struct {
		Type    string `json:"type"`
		Payload string `json:"payload"`
	}, len(events))

	for i, event := range events {
		payloadBytes, err := json.Marshal(event.Payload)
		if err != nil {
			return err
		}

		payload := string(payloadBytes)
		if c.deps.Crypto != nil {
			encryptedPayload, err := c.deps.Crypto.Encrypt(payloadBytes)
			if err != nil {
				return err
			}
			payload = string(encryptedPayload)
		}

		requestEvents[i].Type = event.Type
		requestEvents[i].Payload = payload
	}

	body := PublishEventsRequest{
		Events: requestEvents,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/publish_events", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *NixBusHttpClient) PutSubscriber(subscriberId string, eventType string, config struct {
	MaxRetries  int
	Timeout     int
	Concurrency int
},
) error {
	body := PutSubscriberRequest{
		SubscriberId: subscriberId,
		EventType:    eventType,
		Config: struct {
			MaxRetries  int `json:"max_retries"`
			Timeout     int `json:"timeout"`
			Concurrency int `json:"concurrency"`
		}{
			MaxRetries:  config.MaxRetries,
			Timeout:     config.Timeout,
			Concurrency: config.Concurrency,
		},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/put_subscriber", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *NixBusHttpClient) RemoveAllSubscribers() error {
	resp, err := c.fetchJSON(fmt.Sprintf("%s/remove_all_subscribers", c.baseUrl), "POST", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *NixBusHttpClient) RemoveSubscriber(eventType string, subscriberId string) error {
	body := RemoveSubscriberRequest{
		EventType:    eventType,
		SubscriberId: subscriberId,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.fetchJSON(fmt.Sprintf("%s/remove_subscriber", c.baseUrl), "POST", bodyBytes)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *NixBusHttpClient) deserialize(_ string, event EventResponse) (*FindEventResponse, error) {
	var payload map[string]any

	if c.deps.Crypto != nil {
		decryptedPayload, err := c.deps.Crypto.Decrypt([]byte(event.Payload))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(decryptedPayload, &payload); err != nil {
			return nil, err
		}
	} else {
		if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil {
			return nil, err
		}
	}

	return &FindEventResponse{
		Id:        event.Id,
		Type:      event.Type,
		Payload:   payload,
		CreatedAt: event.CreatedAt,
		UpdatedAt: event.UpdatedAt,
	}, nil
}

func (c *NixBusHttpClient) fetchJSON(url string, method string, body []byte) (*http.Response, error) {
	return c.fetchJSONWithRetry(url, method, body, 0)
}

func (c *NixBusHttpClient) fetchJSONWithRetry(url string, method string, body []byte, retryCount int) (*http.Response, error) {
	const maxRetries = 3

	var req *http.Request
	var err error

	if body != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.opts.Token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if retryCount < maxRetries {
			c.deps.Logger.Debug("NixBusHttpClient", "fetchJSON", map[string]any{
				"message":     fmt.Sprintf("Retrying fetch in 1s (%d/%d)", retryCount+1, maxRetries),
				"url":         url,
				"retry":       retryCount + 1,
				"max_retries": maxRetries,
			})
			time.Sleep(1 * time.Second)
			return c.fetchJSONWithRetry(url, method, body, retryCount+1)
		}
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("%s - %d - %s", url, resp.StatusCode, string(bodyBytes))
	}

	return resp, nil
}
