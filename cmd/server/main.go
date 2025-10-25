package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	// Subscribe to game logs using SubscribeGob
	routingKey := fmt.Sprintf("%s.*", routing.GameLogSlug)
	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic, // exchange
		routing.GameLogSlug,        // queueName (game_logs)
		routingKey,                 // key (game_logs.*)
		pubsub.Durable,             // queueType
		func(gameLog routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")
			err := gamelogic.WriteLog(gameLog)
			if err != nil {
				log.Printf("Failed to write log to disk: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		},
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to game logs: %v", err)
	}
	fmt.Printf("Successfully subscribed to game logs from %s exchange with routing key %s\n", routing.ExchangePerilTopic, routingKey)

	// Print server help to show available commands
	gamelogic.PrintServerHelp()

	// Set up signal handling for graceful shutdown (non-blocking)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start infinite loop for REPL
	for {
		// Check for signals (non-blocking)
		select {
		case <-sigChan:
			fmt.Println("Shutting down server...")
			fmt.Println("Connection closed.")
			return
		default:
			// Continue with REPL
		}

		// Get input from user
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		// Check first word for commands
		command := words[0]
		switch command {
		case "pause":
			fmt.Println("Sending pause message...")
			playingState := routing.PlayingState{IsPaused: true}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playingState)
			if err != nil {
				log.Printf("Failed to publish pause message: %v", err)
			}

		case "resume":
			fmt.Println("Sending resume message...")
			playingState := routing.PlayingState{IsPaused: false}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playingState)
			if err != nil {
				log.Printf("Failed to publish resume message: %v", err)
			}

		case "quit":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Printf("Don't understand command: %s\n", command)
		}
	}
}
