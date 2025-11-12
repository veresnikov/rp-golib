package amqp

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Logger interface {
	Info(...interface{})
	Error(error, ...interface{})
}

type Connection interface {
	Start() error
	Stop() error
	AddChannel(channel Channel)

	Producer(exchangeConfig *ExchangeConfig, queueConfig *QueueConfig, bindConfig *BindConfig) Producer
	Consumer(ctx context.Context, handler Handler, queueConfig *QueueConfig, bindConfig *BindConfig) Consumer
}

type Channel interface {
	Connect(conn *amqp.Connection) error
}

func NewAMQPConnection(appID string, config *ConnectionConfig, logger Logger) Connection {
	return &connection{
		appID:     appID,
		config:    config,
		logger:    logger,
		channelMu: &sync.Mutex{},
	}
}

type connection struct {
	appID  string
	config *ConnectionConfig
	logger Logger

	conn      *amqp.Connection
	channelMu *sync.Mutex
	channels  []Channel
}

func (c *connection) Start() error {
	url := fmt.Sprintf("amqp://%s:%s@%s/", c.config.User, c.config.Password, c.config.Host)

	err := backoff.Retry(func() error {
		connection, cErr := amqp.Dial(url)
		c.conn = connection
		return cErr
	}, newBackOff(c.config.ConnectTimeout))
	if err != nil {
		return err
	}

	if err = c.validateConnection(c.conn); err != nil {
		return err
	}

	err = func() error {
		c.channelMu.Lock()
		defer c.channelMu.Unlock()

		for _, channel := range c.channels {
			if err = channel.Connect(c.conn); err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

	connErrorChan := c.conn.NotifyClose(make(chan *amqp.Error))
	go c.processConnectErrors(connErrorChan)

	return nil
}

func (c *connection) Stop() error {
	return c.conn.Close()
}

func (c *connection) AddChannel(channel Channel) {
	c.channelMu.Lock()
	c.channels = append(c.channels, channel)
	c.channelMu.Unlock()
}

func (c *connection) Producer(exchangeConfig *ExchangeConfig, queueConfig *QueueConfig, bindConfig *BindConfig) Producer {
	producer := NewProducer(c.appID, exchangeConfig, queueConfig, bindConfig, c.logger)
	c.AddChannel(producer)
	return producer
}

func (c *connection) Consumer(ctx context.Context, handler Handler, queueConfig *QueueConfig, bindConfig *BindConfig) Consumer {
	consumer := NewConsumer(ctx, handler, queueConfig, bindConfig, c.logger)
	c.AddChannel(consumer)
	return consumer
}

func (c *connection) validateConnection(conn *amqp.Connection) error {
	if conn == nil {
		return stderrors.New("amqp connection is closed")
	}
	if conn.IsClosed() {
		return stderrors.New("amqp connection is empty")
	}
	return nil
}

func (c *connection) processConnectErrors(ch chan *amqp.Error) {
	err := <-ch
	if err == nil {
		return
	}

	c.logger.Error(err, "AMQP connection error, trying to reconnect")
	for {
		err := c.Start()
		if err == nil {
			c.logger.Info("AMQP connection restored")
			return
		}
		c.logger.Error(err, "failed to reconnect to AMQP")
	}
}

func newBackOff(timeout time.Duration) backoff.BackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()
	const defaultTimeout = 60 * time.Second
	if timeout != 0 {
		exponentialBackOff.MaxElapsedTime = timeout
	} else {
		exponentialBackOff.MaxElapsedTime = defaultTimeout
	}
	exponentialBackOff.MaxInterval = 5 * time.Second
	return exponentialBackOff
}
