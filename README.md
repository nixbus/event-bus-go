# NixBus Go Event Bus

NixBus Event Bus is a secure, robust, and efficient event bus over HTTP. This Go SDK allows you to integrate NixBus into your applications seamlessly, enabling decoupled event-driven architectures. Ideal for microservices, this SDK supports event publishing, subscribing, and processing with ease.

![Awt7xreeDXMZ7WiYiGACiLjm](https://github.com/user-attachments/assets/9ed8978a-3f3f-4280-8ea9-4d3bcd09af2d)

## Features

- **Simple Integration:** Easily add NixBus to your Go project using the NixBus Go module.
- **Secure Communication:** End-to-end encryption ensures your event data is safe.
- **Scalable:** Handle events across multiple microservices and locations.
- **In-Memory Option:** Use the in-memory event bus for smaller applications.
- **Detailed Monitoring:** Monitor your event usage and system performance in real-time via [NixBus dashboard](https://nixbus.com/dashboard).

## Installation

To install the NixBus Event Bus SDK, run the following command:

```bash
go get github.com/nixbus/event-bus-go
```

## Usage

### Subscribing to Events and running the event bus

```go
package main

import (
 "fmt"
 "log"

 eventbus "github.com/nixbus/event-bus-go"
)

func main() {
 nixbus := eventbus.GetNixBusHttp(eventbus.NixBusHttpOptions{
  Token:      "your_token",
  Passphrase: "your_passphrase",
 })

 err := nixbus.Subscribe("event_type", eventbus.SubscriberWithAction{
  NixSubscriber: eventbus.NixSubscriber{
   Id: "a_subscriber_id",
   Config: eventbus.NixSubscriberConfig{
    MaxRetries:  3,
    Timeout:     10,
    Concurrency: 500,
   },
  },
  Action: func(event eventbus.NixEvent) error {
   fmt.Println("Received event:", event)
   // Process the event
   return nil
  },
 })
 if err != nil {
  log.Fatalf("Failed to subscribe: %v", err)
 }

 // Use RunBlocking for a standalone service that handles OS signals
 nixbus.RunBlocking()
}
```

### Subscribing to Events with `deadAction` and running the event bus

```go
package main

import (
 "fmt"
 "log"

 eventbus "github.com/nixbus/event-bus-go"
)

func main() {
 nixbus := eventbus.GetNixBusHttp(eventbus.NixBusHttpOptions{
  Token:      "your_token",
  Passphrase: "your_passphrase",
 })

 err := nixbus.Subscribe("event_type", eventbus.SubscriberWithAction{
  NixSubscriber: eventbus.NixSubscriber{
   Id: "a_subscriber_id",
   Config: eventbus.NixSubscriberConfig{
    MaxRetries:  3,
    Timeout:     10,
    Concurrency: 500,
   },
  },
  Action: func(event eventbus.NixEvent) error {
   fmt.Println("Processing event:", event)
   // Your business logic
   return nil
  },
  DeadAction: func(event eventbus.NixEvent) error {
   fmt.Println("Dead event encountered:", event)
   // Handle events that reach maxRetries
   return nil
  },
 })
 if err != nil {
  log.Fatalf("Failed to subscribe: %v", err)
 }

 nixbus.RunBlocking()
}
```

### Publishing events

```go
package main

import (
 "log"

 eventbus "github.com/nixbus/event-bus-go"
)

func main() {
 nixbus := eventbus.GetNixBusHttp(eventbus.NixBusHttpOptions{
  Token:      "your_token",
  Passphrase: "your_passphrase",
 })

 err := nixbus.Publish(eventbus.NixNewEvent{
  Type:    "event_type",
  Payload: map[string]any{"welcome": "to the event bus"},
 })
 if err != nil {
  log.Fatalf("Failed to publish event: %v", err)
 }
}
```

## Key Concepts

### Timeout

- Events must be processed by the subscriber's `Action` within the configured `Timeout`.
- If an event is not processed (marked as finished or failed) within this timeout, it will be re-delivered for processing.
- This ensures no events are lost, even in cases of temporary subscriber issues.

### MaxRetries

- Defines the maximum number of retry attempts for a subscriber's `Action`.
- If the `Action` fails repeatedly and reaches the `MaxRetries` limit, the event is considered a "dead event."
- Dead events trigger the optional `DeadAction`, providing a chance for compensatory handling or logging.

### Automatic Event Marking

The library automatically manages the lifecycle of events:

- **Finished**: If the `Action` completes successfully without errors, the event is marked as finished and will not be re-delivered.
- **Failed**: If the `Action` returns an error or fails to process the event, the event is marked as failed and remains eligible for retry until MaxRetries is reached.
- **DeadAction Handling**:
  - If the event exceeds `MaxRetries`, the `DeadAction` (if defined) is triggered.
  - After the `DeadAction` runs (whether it succeeds or returns an error), the event is marked as finished and will not be re-delivered.

### Behavior When `DeadAction` Is Not Defined

- If no `DeadAction` is defined for the subscriber, the event will remain unmarked after reaching `MaxRetries`.
- This means:
  - The event will stay in the dead-letter queue but will not be processed further.
  - Increasing the subscriber's `MaxRetries` will allow the event to be reprocessed. The action will run again for the event until it is successfully processed or reaches the new `MaxRetries`.

### DeadAction

- **Purpose**: Allows you to define custom handling for events that exceed their retry limits. Common use cases include logging, notifying administrators, or archiving data for manual inspection.
- **Execution**: Runs only once when an event reaches the retry limit. After execution, the event is marked as finished.
- Optional: If no `DeadAction` is defined, events remain in the dead-letter queue and can be reprocessed by increasing the `MaxRetries`.

## Run Modes

The NixBus Go implementation provides two ways to run the event bus:

1. **Run()**: Non-blocking method that starts the event bus in the current goroutine. Use this when integrating with an existing application that manages its own lifecycle.

2. **RunBlocking()**: Blocking method that handles OS signals (Ctrl+C) for graceful shutdown. Ideal for standalone services.

```go
// For standalone services that should terminate on Ctrl+C
nixbus.RunBlocking()

// For integration with existing applications
go nixbus.Run()
// Keep your application running...
```

## API Documentation

For more detailed information on using the [NixBus HTTP API](https://nixbus.com/api), or if you want to create your own implementation or build SDKs in other languages, please refer to [NixBus API documentation](https://nixbus.com/api).
