package pubsub

import (
	"context"
	"encoding/json"

	"github.com/rabbitmq/amqp091-go"
)

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
