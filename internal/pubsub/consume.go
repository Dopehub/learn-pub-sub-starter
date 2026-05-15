package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType struct {
	Durable    bool
	Exclusive  bool
	AutoDelete bool
}

var Durable SimpleQueueType = SimpleQueueType{
	Durable:    true,
	Exclusive:  false,
	AutoDelete: false,
}

var Transient SimpleQueueType = SimpleQueueType{
	Durable:    false,
	Exclusive:  true,
	AutoDelete: true,
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	pubChan, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	queue, err := pubChan.QueueDeclare(queueName, queueType.Durable, queueType.AutoDelete, queueType.Exclusive, false, nil)
	if err != nil {
		return pubChan, amqp.Queue{}, err
	}

	if err = pubChan.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, err
	}

	return pubChan, queue, nil
}
