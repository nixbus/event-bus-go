package domain

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Logger interface {
	Info(namespace string, name string, data map[string]any)
	Error(namespace string, name string, data map[string]any)
	Debug(namespace string, name string, data map[string]any)
}

type EventBusDeps struct {
	Events NixEvents
	Logger Logger
}

type NixEventBus struct {
	deps                      EventBusDeps
	subscribersActions        map[string]NixSubscriberAction
	subscribersDeadActions    map[string]NixSubscriberAction
	hasSubscriberActionFailed map[string]bool
	hasSubscriberDeadEvents   map[string]bool
	mutex                     sync.RWMutex
	running                   bool
	done                      chan struct{}
}

func NewNixEventBus(deps EventBusDeps) *NixEventBus {
	return &NixEventBus{
		deps:                      deps,
		subscribersActions:        make(map[string]NixSubscriberAction),
		subscribersDeadActions:    make(map[string]NixSubscriberAction),
		hasSubscriberActionFailed: make(map[string]bool),
		hasSubscriberDeadEvents:   make(map[string]bool),
		mutex:                     sync.RWMutex{},
		running:                   false,
		done:                      make(chan struct{}),
	}
}

type SubscriberWithAction struct {
	NixSubscriber
	Action     NixSubscriberAction
	DeadAction NixSubscriberAction
}

func (eb *NixEventBus) Subscribe(eventType string, subscriber SubscriberWithAction) error {
	eb.mutex.Lock()
	defer eb.mutex.Unlock()

	eb.subscribersActions[string(subscriber.Id)] = subscriber.Action
	if subscriber.DeadAction != nil {
		eb.subscribersDeadActions[string(subscriber.Id)] = subscriber.DeadAction
		eb.hasSubscriberDeadEvents[string(subscriber.Id)] = true
	}

	err := eb.deps.Events.Subscribe(eventType, subscriber.NixSubscriber)
	if err != nil {
		eb.deps.Logger.Error("EventBus", "subscribe", map[string]any{"error": err})
		return err
	}
	return nil
}

func (eb *NixEventBus) Unsubscribe(eventType string, subscriberId NixSubscriberId) error {
	err := eb.deps.Events.Unsubscribe(eventType, string(subscriberId))
	if err != nil {
		eb.deps.Logger.Error("EventBus", "unsubscribe", map[string]any{"error": err})
		return err
	}
	return nil
}

func (eb *NixEventBus) UnsubscribeAll() error {
	eb.mutex.Lock()
	eb.subscribersActions = make(map[string]NixSubscriberAction)
	eb.mutex.Unlock()

	err := eb.deps.Events.UnsubscribeAll()
	if err != nil {
		eb.deps.Logger.Error("EventBus", "unsubscribeAll", map[string]any{"error": err})
		return err
	}
	return nil
}

func (eb *NixEventBus) Publish(event NixNewEvent) error {
	err := eb.deps.Events.Put(event)
	if err != nil {
		eb.deps.Logger.Error("EventBus", "publish", map[string]any{"error": err})
		return err
	}
	return nil
}

func (eb *NixEventBus) Run() {
	eb.mutex.Lock()
	if eb.running {
		eb.mutex.Unlock()
		return
	}
	eb.running = true
	eb.mutex.Unlock()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	eb.deps.Logger.Info("EventBus", "run", map[string]any{
		"status": "started",
		"date":   time.Now().UTC().Format(time.RFC3339),
	})

	for eb.isRunning() {
		select {
		case <-ticker.C:
			if err := eb.runScheduler(); err != nil {
				eb.deps.Logger.Error("EventBus", "run", map[string]any{
					"error": err.Error(),
					"date":  time.Now().UTC().Format(time.RFC3339),
				})
			}
		case <-eb.done:
			eb.deps.Logger.Info("EventBus", "run", map[string]any{
				"status": "stopping",
				"date":   time.Now().UTC().Format(time.RFC3339),
			})
			return
		}
	}

	eb.deps.Logger.Info("EventBus", "run", map[string]any{
		"status": "stopped",
		"date":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (eb *NixEventBus) RunBlocking() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go eb.Run()

	<-stop

	eb.Stop()
}

func (eb *NixEventBus) Stop() {
	eb.mutex.Lock()
	if !eb.running {
		eb.mutex.Unlock()
		return
	}
	eb.running = false
	close(eb.done)
	eb.mutex.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		timeout := time.After(5 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if !eb.hasOngoingTasks() {
					return
				}
			case <-timeout:
				eb.deps.Logger.Error("EventBus", "stop", map[string]any{
					"error": "timeout waiting for tasks to complete",
					"date":  time.Now().UTC().Format(time.RFC3339),
				})
				return
			}
		}
	}()
	wg.Wait()
}

func (eb *NixEventBus) hasOngoingTasks() bool {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()

	return len(eb.hasSubscriberActionFailed) > 0 || len(eb.hasSubscriberDeadEvents) > 0
}

func (eb *NixEventBus) isRunning() bool {
	eb.mutex.RLock()
	defer eb.mutex.RUnlock()
	return eb.running
}

func (eb *NixEventBus) runScheduler() error {
	subscribers, err := eb.deps.Events.GetSubscribers()
	if err != nil {
		eb.deps.Logger.Error("EventBus", "runScheduler", map[string]any{"error": err})
		return err
	}

	eb.deps.Logger.Debug("EventBus", "runScheduler", map[string]any{
		"subscribers": len(subscribers),
		"date":        time.Now().UTC().Format(time.RFC3339),
	})

	var wg sync.WaitGroup
	for _, s := range subscribers {
		wg.Add(1)
		go func(subscriber NixSubscriber) {
			defer wg.Done()
			err := eb.runSubscriber(subscriber)
			if err != nil {
				eb.deps.Logger.Error("EventBus", "runScheduler", map[string]any{
					"error": err,
				})
			}
		}(s)
	}
	wg.Wait()

	return nil
}

func (eb *NixEventBus) runSubscriber(subscriber NixSubscriber) error {
	events, err := eb.deps.Events.FindNextEventsFor(subscriber)
	if err != nil {
		eb.deps.Logger.Error("EventBus", "runSubscriber", map[string]any{
			"method":        "runSubscriber",
			"subscriber_id": subscriber.Id,
			"error":         err,
		})
		return err
	}

	deadEvents, err := eb.findDeadEventsFor(subscriber)
	if err != nil {
		eb.deps.Logger.Error("EventBus", "runSubscriber", map[string]any{
			"method":        "runSubscriber",
			"subscriber_id": subscriber.Id,
			"error":         err,
		})
		return err
	}

	var wg sync.WaitGroup

	// events length
	eb.deps.Logger.Info("EventBus", "runSubscriber", map[string]any{
		"events":        len(events),
		"dead_events":   len(deadEvents),
		"subscriber_id": subscriber.Id,
	})

	for _, event := range events {
		wg.Add(1)
		go func(e NixEvent) {
			defer wg.Done()
			eb.deps.Logger.Info("EventBus", "runSubscriber", map[string]any{
				"event_id":      e.Id,
				"event_type":    e.Type,
				"subscriber_id": subscriber.Id,
			})
			err := eb.runSubscriberAction(e, subscriber)
			if err != nil {
				eb.deps.Logger.Error("EventBus", "runSubscriber", map[string]any{
					"event_id":      e.Id,
					"subscriber_id": subscriber.Id,
					"error":         err,
				})
			}
		}(event)
	}

	for _, event := range deadEvents {
		wg.Add(1)
		go func(e NixEvent) {
			defer wg.Done()
			eb.deps.Logger.Info("EventBus", "[deadEvents] runSubscriber", map[string]any{
				"event_id":      e.Id,
				"event_type":    e.Type,
				"subscriber_id": subscriber.Id,
			})
			err := eb.runDeadSubscriberAction(e, subscriber)
			if err != nil {
				eb.deps.Logger.Error("EventBus", "[deadEvents] runSubscriber", map[string]any{
					"event_id":      e.Id,
					"subscriber_id": subscriber.Id,
					"error":         err,
				})
			}
		}(event)
	}

	wg.Wait()
	return nil
}

func (eb *NixEventBus) runSubscriberAction(event NixEvent, subscriber NixSubscriber) error {
	eb.mutex.RLock()
	action, exists := eb.subscribersActions[string(subscriber.Id)]
	eb.mutex.RUnlock()

	if !exists {
		return nil
	}

	err := action(event)
	if err != nil {
		markErr := eb.deps.Events.MarkAsFailed(event, subscriber)
		if markErr != nil {
			eb.deps.Logger.Error("EventBus", "runSubscriberAction", map[string]any{
				"event_id":      event.Id,
				"subscriber_id": subscriber.Id,
				"error":         markErr,
			})
		}
		eb.enableFindDeadEventsFor(subscriber)
		eb.deps.Logger.Error("EventBus", "runSubscriberAction", map[string]any{
			"event_id":      event.Id,
			"subscriber_id": subscriber.Id,
			"error":         err.Error(),
		})
		return err
	}

	markErr := eb.deps.Events.MarkAsFinished(event, subscriber)
	if markErr != nil {
		eb.deps.Logger.Error("EventBus", "runSubscriberAction", map[string]any{
			"event_id":      event.Id,
			"subscriber_id": subscriber.Id,
			"error":         markErr,
		})
		return markErr
	}

	eb.deps.Logger.Info("EventBus", "runSubscriberAction", map[string]any{
		"event_id":      event.Id,
		"event_type":    event.Type,
		"subscriber_id": subscriber.Id,
	})

	return nil
}

func (eb *NixEventBus) runDeadSubscriberAction(event NixEvent, subscriber NixSubscriber) error {
	eb.mutex.RLock()
	action, exists := eb.subscribersDeadActions[string(subscriber.Id)]
	eb.mutex.RUnlock()

	if !exists {
		return nil
	}

	err := action(event)
	if err != nil {
		markErr := eb.deps.Events.MarkAsFinished(event, subscriber)
		if markErr != nil {
			eb.deps.Logger.Error("EventBus", "runDeadSubscriberAction", map[string]any{
				"event_id":      event.Id,
				"subscriber_id": subscriber.Id,
				"error":         markErr,
			})
			return markErr
		}
		eb.deps.Logger.Error("EventBus", "runDeadSubscriberAction", map[string]any{
			"event_id":      event.Id,
			"subscriber_id": subscriber.Id,
			"error":         err,
		})
		return err
	}

	markErr := eb.deps.Events.MarkAsFinished(event, subscriber)
	if markErr != nil {
		eb.deps.Logger.Error("EventBus", "runDeadSubscriberAction", map[string]any{
			"event_id":      event.Id,
			"subscriber_id": subscriber.Id,
			"error":         markErr,
		})
		return markErr
	}

	eb.deps.Logger.Info("EventBus", "runDeadSubscriberAction", map[string]any{
		"event_id":      event.Id,
		"event_type":    event.Type,
		"subscriber_id": subscriber.Id,
	})

	return nil
}

func (eb *NixEventBus) enableFindDeadEventsFor(subscriber NixSubscriber) {
	time.AfterFunc(time.Duration(subscriber.Config.Timeout)*time.Second, func() {
		eb.mutex.Lock()
		eb.hasSubscriberActionFailed[string(subscriber.Id)] = true
		eb.mutex.Unlock()
	})
}

func (eb *NixEventBus) findDeadEventsFor(subscriber NixSubscriber) ([]NixEvent, error) {
	eb.mutex.RLock()
	actionFailed := eb.hasSubscriberActionFailed[string(subscriber.Id)]
	deadEvents := eb.hasSubscriberDeadEvents[string(subscriber.Id)]
	_, hasDeadAction := eb.subscribersDeadActions[string(subscriber.Id)]
	eb.mutex.RUnlock()

	if (actionFailed || deadEvents) && hasDeadAction {
		eb.mutex.Lock()
		eb.hasSubscriberDeadEvents[string(subscriber.Id)] = true
		eb.mutex.Unlock()

		events, err := eb.deps.Events.FindDeadEventsFor(subscriber)
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			eb.mutex.Lock()
			eb.hasSubscriberDeadEvents[string(subscriber.Id)] = false
			eb.mutex.Unlock()
		}

		eb.mutex.Lock()
		eb.hasSubscriberActionFailed[string(subscriber.Id)] = false
		eb.mutex.Unlock()

		return events, nil
	}

	return []NixEvent{}, nil
}
