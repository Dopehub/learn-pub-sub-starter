package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
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
	table amqp.Table,
) (*amqp.Channel, amqp.Queue, error) {
	pubChan, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	queue, err := pubChan.QueueDeclare(queueName, queueType.Durable, queueType.AutoDelete, queueType.Exclusive, false, table)
	if err != nil {
		return pubChan, amqp.Queue{}, err
	}

	if err = pubChan.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, err
	}

	return pubChan, queue, nil
}

func Subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {

	ch, queue, err := DeclareAndBind(conn,
		exchange,
		queueName,
		key,
		queueType,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		})

	if err != nil {
		return fmt.Errorf("Could not bind to queue: %w", err)
	}

	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Could not get delivery channel: %w", err)
	}

	go func() {
		defer ch.Close()

		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("Could not Unmarshal message: %v", err)
				continue
			}
			ackT := handler(target)
			switch ackT {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("Nack Requeue")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("Nack Discard")
			}
		}
	}()

	return nil
}


func UnmarshallerGob[T any](data []byte) (T, error) {
	var target T
	decoder := gob.NewDecoder(bytes.NewBuffer(data))
	if err := decoder.Decode(&target); err != nil {
		return target, err
	}
	return target, nil
}

func UnmarshallerJson[T any](data []byte) (T, error) {
	var target T
	if err := json.Unmarshal(data, &target); err != nil {
		return target, err
	}
	return target, nil
}
