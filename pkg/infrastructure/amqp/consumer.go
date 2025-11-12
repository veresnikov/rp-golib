package amqp

import (
	"context"
	stderrors "errors"

	amqp "github.com/rabbitmq/amqp091-go"

	liberr "gitea.xscloud.ru/xscloud/golib/pkg/internal/errors"
)

type Handler func(ctx context.Context, delivery Delivery) error

type Consumer interface {
	Channel
}

func NewConsumer(
	ctx context.Context,
	handler Handler,
	queueConfig *QueueConfig,
	bindConfig *BindConfig,
	logger Logger,
) Consumer {
	if queueConfig == nil {
		panic("queue config is required")
	}
	return &consumer{
		ctx:         ctx,
		handler:     handler,
		queueConfig: queueConfig,
		bindConfig:  bindConfig,
		logger:      logger,
	}
}

type consumer struct {
	ctx         context.Context
	handler     Handler
	queueConfig *QueueConfig
	bindConfig  *BindConfig

	logger  Logger
	conn    *amqp.Connection
	channel *amqp.Channel
}

func (c *consumer) Connect(conn *amqp.Connection) (err error) {
	c.conn = conn

	channel, err := c.conn.Channel()
	if err != nil {
		return err
	}
	err = c.validateChannel(channel)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = liberr.Join(err, channel.Close())
		}
	}()

	if c.queueConfig != nil {
		err = queueDeclare(*c.queueConfig, channel)
		if err != nil {
			return err
		}
	}

	if c.bindConfig != nil {
		err = bindDeclare(*c.bindConfig, channel)
		if err != nil {
			return err
		}
	}

	connErrorChan := channel.NotifyClose(make(chan *amqp.Error))
	go c.processConnectErrors(connErrorChan)

	c.channel = channel

	return c.consume()
}

func (c *consumer) consume() error {
	deliveriesChan, err := c.channel.Consume(c.queueConfig.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveriesChan {
			err = c.handler(c.ctx, Delivery{
				RoutingKey:    delivery.RoutingKey,
				CorrelationID: delivery.CorrelationId,
				ContentType:   delivery.ContentType,
				Type:          delivery.Type,
				Body:          delivery.Body,
			})
			if err != nil {
				_ = delivery.Ack(false)
			} else {
				_ = delivery.Nack(false, true)
			}
		}
	}()

	return nil
}

func (c *consumer) validateChannel(channel *amqp.Channel) error {
	if channel == nil {
		return stderrors.New("amqp channel is empty")
	}
	if channel.IsClosed() {
		return stderrors.New("amqp channel is closed")
	}
	return nil
}

func (c *consumer) processConnectErrors(ch chan *amqp.Error) {
	err := <-ch
	if err == nil {
		return
	}

	c.logger.Error(err, "AMQP channel error, trying to reconnect")
	for {
		err := c.Connect(c.conn)
		if err == nil {
			c.logger.Info("AMQP channel restored")
			return
		}
		c.logger.Error(err, "failed to reconnect to AMQP channel")
	}
}
