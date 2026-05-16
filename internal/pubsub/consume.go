package pubsub

import (
	"encoding/json"
	"fmt"

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

func DeclareAndBind(conn *amqp.Connection,
	exchange, queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {

	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("Could not bind to queue: %w", err)
	}

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Could not get delivery channel: %w", err)
	}

	go func() {
		defer ch.Close()
		var target T
		for msg := range msgs {
			if err := json.Unmarshal(msg.Body, &target); err != nil {
				fmt.Printf("Could not Unmarshal message: %w", err)
				continue
			}
			handler(target)
			msg.Ack(false)
		}
	}()

	return nil
}
