package pubsub

import (
	"context"
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType is an enum to represent queue types
type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

// DeclareAndBind creates a channel, declares a queue, and binds it to an exchange
func DeclareAndBind(
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp091.Channel, amqp091.Queue, error) {
	// Create a new channel
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp091.Queue{}, err
	}

	// Set queue parameters based on queueType
	var durable, autoDelete, exclusive bool
	if queueType == Durable {
		durable = true
		autoDelete = false
		exclusive = false
	} else { // Transient
		durable = false
		autoDelete = true
		exclusive = true
	}

	// Declare the queue
	queue, err := ch.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autoDelete, // autoDelete
		exclusive,  // exclusive
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		ch.Close()
		return nil, amqp091.Queue{}, err
	}

	// Bind the queue to the exchange
	err = ch.QueueBind(
		queue.Name, // queue
		key,        // key
		exchange,   // exchange
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		ch.Close()
		return nil, amqp091.Queue{}, err
	}

	return ch, queue, nil
}

// PublishJSON publishes a JSON-marshaled value to RabbitMQ
func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	// Marshal the val to JSON bytes
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	// Use the channel's PublishWithContext method
	return ch.PublishWithContext(
		context.Background(), // ctx
		exchange,             // exchange
		key,                  // routing key
		false,                // mandatory
		false,                // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        jsonBytes,
		},
	)
}
