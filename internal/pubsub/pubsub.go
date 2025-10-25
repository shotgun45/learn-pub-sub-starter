package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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
		amqp091.Table{
			"x-dead-letter-exchange": routing.ExchangePerilDLX,
		}, // args
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

// PublishGob publishes a gob-encoded value to RabbitMQ
func PublishGob[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	// Create a buffer to encode the value into
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	// Encode the val to gob bytes
	err := encoder.Encode(val)
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
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
}

// subscribe is a helper function to share duplicate code between SubscribeJSON and SubscribeGob
func subscribe[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	// Call DeclareAndBind to ensure the queue exists and is bound
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
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
			// Unmarshal the message body into type T using the provided unmarshaller
			message, err := unmarshaller(delivery.Body)
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

// SubscribeJSON subscribes to a queue and handles JSON messages with a generic handler
func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var message T
		err := json.Unmarshal(data, &message)
		return message, err
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

// SubscribeGob subscribes to a queue and handles gob-encoded messages with a generic handler
func SubscribeGob[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var message T
		buf := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buf)
		err := decoder.Decode(&message)
		return message, err
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}
