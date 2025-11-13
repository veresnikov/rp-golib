package amqp

import (
	"context"
	stderrors "errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	liberr "gitea.xscloud.ru/xscloud/golib/pkg/internal/errors"
)

type Delivery struct {
	RoutingKey    string
	CorrelationID string
	ContentType   string
	Type          string
	Body          []byte
}

type Producer interface {
	Channel
	Publish(ctx context.Context, delivery Delivery) error
}

func NewProducer(
	appID string,
	exchangeConfig *ExchangeConfig,
	queueConfig *QueueConfig,
	bindConfig *BindConfig,
	logger Logger,
) Producer {
	if exchangeConfig == nil && queueConfig == nil {
		panic("exchange or queue config is required")
	}
	return &producer{
		appID:          appID,
		exchangeConfig: exchangeConfig,
		queueConfig:    queueConfig,
		bindConfig:     bindConfig,
		logger:         logger,
	}
}

type producer struct {
	appID          string
	exchangeConfig *ExchangeConfig
	queueConfig    *QueueConfig
	bindConfig     *BindConfig
	logger         Logger

	conn    *amqp.Connection
	channel *amqp.Channel
}

func (p *producer) Connect(conn *amqp.Connection) (err error) {
	p.conn = conn

	channel, err := p.conn.Channel()
	if err != nil {
		return err
	}
	err = p.validateChannel(channel)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = liberr.Join(err, channel.Close())
		}
	}()

	if p.exchangeConfig != nil {
		err = exchangeDeclare(*p.exchangeConfig, channel)
		if err != nil {
			return err
		}
	}

	if p.queueConfig != nil {
		err = queueDeclare(*p.queueConfig, channel)
		if err != nil {
			return err
		}
	}

	if p.bindConfig != nil {
		err = bindDeclare(*p.bindConfig, channel)
		if err != nil {
			return err
		}
	}

	err = p.channel.Confirm(false)
	if err != nil {
		return err
	}

	connErrorChan := channel.NotifyClose(make(chan *amqp.Error))
	go p.processConnectErrors(connErrorChan)

	p.channel = channel

	return nil
}

func (p *producer) Publish(ctx context.Context, delivery Delivery) error {
	err := p.validateChannel(p.channel)
	if err != nil {
		return err
	}

	var exchange string
	if p.exchangeConfig != nil {
		exchange = p.exchangeConfig.Name
	}
	deferredConfirmation, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		delivery.RoutingKey,
		true,
		false,
		amqp.Publishing{
			ContentType:   delivery.ContentType,
			DeliveryMode:  amqp.Persistent,
			CorrelationId: delivery.CorrelationID,
			Timestamp:     time.Now(),
			Type:          delivery.Type,
			AppId:         p.appID,
			Body:          delivery.Body,
		},
	)
	if err != nil {
		return err
	}
	if deferredConfirmation == nil {
		return nil
	}
	publishOk, err := deferredConfirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !publishOk {
		return stderrors.New("failed to publish delivery")
	}
	return nil
}

func (p *producer) validateChannel(channel *amqp.Channel) error {
	if channel == nil {
		return stderrors.New("amqp channel is empty")
	}
	if channel.IsClosed() {
		return stderrors.New("amqp channel is closed")
	}
	return nil
}

func (p *producer) processConnectErrors(ch chan *amqp.Error) {
	err := <-ch
	if err == nil {
		return
	}

	p.logger.Error(err, "AMQP channel error, trying to reconnect")
	for {
		err := p.Connect(p.conn)
		if err == nil {
			p.logger.Info("AMQP channel restored")
			return
		}
		p.logger.Error(err, "failed to reconnect to AMQP channel")
	}
}
