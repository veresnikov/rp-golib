package amqp

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectionConfig struct {
	User           string
	Password       string
	Host           string
	ConnectTimeout time.Duration
}

type ExchangeConfig struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type QoSConfig struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type BindConfig struct {
	QueueName    string
	ExchangeName string
	RoutingKeys  []string
	NoWait       bool
	Args         amqp.Table
}

func exchangeDeclare(config ExchangeConfig, channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		config.Name,
		config.Kind,
		config.Durable,
		config.AutoDelete,
		config.Internal,
		config.NoWait,
		config.Args,
	)
}

func queueDeclare(config QueueConfig, channel *amqp.Channel) error {
	_, err := channel.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		config.Args,
	)
	return err
}

func bindDeclare(config BindConfig, channel *amqp.Channel) error {
	for _, routingKey := range config.RoutingKeys {
		err := channel.QueueBind(config.QueueName, routingKey, config.ExchangeName, config.NoWait, config.Args)
		if err != nil {
			return err
		}
	}
	return nil
}

func qosDeclare(config QoSConfig, channel *amqp.Channel) error {
	return channel.Qos(config.PrefetchCount, config.PrefetchSize, config.Global)
}
