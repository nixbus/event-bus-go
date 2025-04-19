package eventbus

import (
	cryptolib "github.com/nixbus/crypto-go"
	cryptolibdomain "github.com/nixbus/crypto-go/core/domain"
	"github.com/nixbus/event-bus-go/core/domain"
	"github.com/nixbus/event-bus-go/core/infrastructure"
)

type NixBusInMemoryOptions struct {
	Log infrastructure.LogLevel
}

var _nixBusInMemory *domain.NixEventBus

func GetNixBusInMemory(options NixBusInMemoryOptions) *domain.NixEventBus {
	if _nixBusInMemory != nil {
		return _nixBusInMemory
	}
	_nixBusInMemory = CreateNixBusInMemory(options)
	return _nixBusInMemory
}

func CreateNixBusInMemory(options NixBusInMemoryOptions) *domain.NixEventBus {
	events := infrastructure.NewNixEventsInMemory()
	logger := infrastructure.NewLogger(options.Log)
	return domain.NewNixEventBus(domain.EventBusDeps{
		Events: events,
		Logger: logger,
	})
}

type NixBusHttpOptions struct {
	Passphrase       string
	Token            string
	ClientEncryption bool
	BaseUrl          string
	Log              infrastructure.LogLevel
}

var _nixBusHttp *domain.NixEventBus

func GetNixBusHttp(options NixBusHttpOptions) *domain.NixEventBus {
	if _nixBusHttp != nil {
		return _nixBusHttp
	}
	_nixBusHttp = CreateNixBusHttp(options)
	return _nixBusHttp
}

func CreateNixBusHttp(options NixBusHttpOptions) *domain.NixEventBus {
	logger := infrastructure.NewLogger(options.Log)

	var crypto *cryptolib.NixBusCrypto
	if options.ClientEncryption {
		defaultPassphraseVersion := "v1"
		crypto = cryptolib.CreateNixBusCrypto(defaultPassphraseVersion, []cryptolibdomain.Passphrase{
			{Version: defaultPassphraseVersion, Phrase: options.Passphrase},
		})
	}

	client := infrastructure.NewNixBusHttpClient(
		infrastructure.NixBusHttpClientDeps{
			Crypto: crypto,
			Logger: logger,
		},
		infrastructure.NixBusHttpClientOptions{
			Token:   options.Token,
			BaseUrl: options.BaseUrl,
		},
	)

	events := infrastructure.NewNixEventsHttp(infrastructure.NixEventsHttpDeps{
		Client: client,
	})

	return domain.NewNixEventBus(domain.EventBusDeps{
		Events: events,
		Logger: logger,
	})
}

type (
	NixEvent             = domain.NixEvent
	NixNewEvent          = domain.NixNewEvent
	NixSubscriber        = domain.NixSubscriber
	NixSubscriberAction  = domain.NixSubscriberAction
	NixSubscriberId      = domain.NixSubscriberId
	NixSubscriberConfig  = domain.NixSubscriberConfig
	NixEventBus          = domain.NixEventBus
	SubscriberWithAction = domain.SubscriberWithAction
)

type (
	NixEventsInMemory = infrastructure.NixEventsInMemory
	NixEventsHttp     = infrastructure.NixEventsHttp
	NixBusHttpClient  = infrastructure.NixBusHttpClient
	Logger            = infrastructure.Logger
	LogLevel          = infrastructure.LogLevel
)

var EventIdIsRequired = domain.EventIdIsRequired
