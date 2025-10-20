package pubsub

import (
	"context"
	"encoding/json"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType is an enum to represent queue types
type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

// AckType represents the acknowledgment type for message handling
type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

// SubscribeJSON subscribes to a queue and handles JSON messages with a generic handler
func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	// Call DeclareAndBind to ensure the queue exists and is bound
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	// Get a channel of deliveries by consuming from the queue
	deliveries, err := ch.Consume(
		queueName, // queue
		"",        // consumer (empty string for auto-generated)
		false,     // autoAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // args
	)
	if err != nil {
		ch.Close()
		return err
	}

	// Start a goroutine to handle messages
	go func() {
		defer ch.Close()
		for delivery := range deliveries {
			// Unmarshal the message body into type T
			var message T
			err := json.Unmarshal(delivery.Body, &message)
			if err != nil {
				// Log error and discard message
				log.Printf("Failed to unmarshal message: %v", err)
				delivery.Nack(false, false)
				log.Println("Message nacked (discarded) due to unmarshal error")
				continue
			}

			// Call the handler function with the unmarshaled message
			ackType := handler(message)

			// Handle acknowledgment based on the returned AckType
			switch ackType {
			case Ack:
				delivery.Ack(false)
				log.Println("Message acknowledged (Ack)")
			case NackRequeue:
				delivery.Nack(false, true)
				log.Println("Message nacked (requeued)")
			case NackDiscard:
				delivery.Nack(false, false)
				log.Println("Message nacked (discarded)")
			default:
				// Default to Ack if unknown type
				delivery.Ack(false)
				log.Printf("Unknown AckType %v, defaulting to Ack", ackType)
			}
		}
	}()

	return nil
}
