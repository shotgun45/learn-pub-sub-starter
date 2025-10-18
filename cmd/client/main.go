package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	// Declare connection string
	connectionString := "amqp://guest:guest@localhost:5672/"

	// Create connection to RabbitMQ
	conn, err := amqp091.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Successfully connected to RabbitMQ!")

	// Use ClientWelcome to get username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get username: %v", err)
	}

	// Create queue name: pause.username
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)

	// Declare and bind a transient queue
	ch, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect, // exchange
		queueName,                   // queueName
		routing.PauseKey,            // key (routing key)
		pubsub.Transient,            // queueType
	)
	if err != nil {
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}
	defer ch.Close()

	fmt.Printf("Queue %s declared and bound successfully!\n", queue.Name)

	// Create a new game state
	gameState := gamelogic.NewGameState(username)

	// Start REPL loop
	for {
		// Get input from user
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		// Check first word for commands
		command := words[0]
		switch command {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
			}

		case "move":
			_, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
			} else {
				fmt.Println("Move successful!")
			}

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}
