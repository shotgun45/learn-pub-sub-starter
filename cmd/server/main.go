package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	// Declare connection string
	connectionString := "amqp://guest:guest@localhost:5672/"

	// Create connection to RabbitMQ
	conn, err := amqp091.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ!")

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	// Publish a pause message using PublishJSON
	playingState := routing.PlayingState{IsPaused: true}
	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playingState)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}
	fmt.Println("Published pause message to RabbitMQ!")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for signal
	<-sigChan

	fmt.Println("Shutting down server...")
	fmt.Println("Connection closed.")
}
