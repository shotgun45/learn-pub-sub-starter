package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

// handlerPause creates a handler function for pause/resume messages
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack // Always acknowledge pause messages
	}
}

// handlerMove creates a handler function for army move messages
func handlerMove(gs *gamelogic.GameState, pubCh *amqp091.Channel, username string) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		// Determine acknowledgment type based on move outcome
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// Create and publish war recognition message
			warMessage := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}

			// Publish to war exchange with routing key war.username
			warRoutingKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, username)
			err := pubsub.PublishJSON(pubCh, routing.ExchangePerilTopic, warRoutingKey, warMessage)
			if err != nil {
				log.Printf("Failed to publish war message: %v", err)
				return pubsub.NackRequeue // Requeue if publishing failed
			}

			return pubsub.Ack // Acknowledge if publishing succeeded
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

// handlerWar creates a handler function for war recognition messages
func handlerWar(gs *gamelogic.GameState, pubCh *amqp091.Channel, username string) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		// Determine acknowledgment type based on war outcome
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeDraw:
			// Publish game log based on outcome
			return publishGameLog(pubCh, outcome, winner, loser, rw.Attacker.Username)
		default:
			fmt.Printf("Error: Unknown war outcome: %v\n", outcome)
			return pubsub.NackDiscard
		}
	}
}

// publishGameLog publishes a game log message based on war outcome
func publishGameLog(pubCh *amqp091.Channel, outcome gamelogic.WarOutcome, winner, loser, initiatorUsername string) pubsub.AckType {
	var message string

	switch outcome {
	case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
		message = fmt.Sprintf("%s won a war against %s", winner, loser)
	case gamelogic.WarOutcomeDraw:
		message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
	default:
		// Should not reach here based on the calling logic, but handle gracefully
		return pubsub.Ack
	}

	// Create GameLog struct
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    initiatorUsername,
	}

	// Publish using gob encoding
	routingKey := fmt.Sprintf("%s.%s", routing.GameLogSlug, initiatorUsername)
	err := pubsub.PublishGob(pubCh, routing.ExchangePerilTopic, routingKey, gameLog)

	if err != nil {
		log.Printf("Failed to publish game log: %v", err)
		return pubsub.NackRequeue
	}

	return pubsub.Ack
}

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

	// Subscribe to pause messages using SubscribeJSON
	err = pubsub.SubscribeJSON(
		conn,                        // connection
		routing.ExchangePerilDirect, // exchange
		queueName,                   // queue name (pause.username)
		routing.PauseKey,            // routing key (pause)
		pubsub.Transient,            // queue type
		handlerPause(gameState),     // handler function
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to pause messages: %v", err)
	}

	// Create a publishing channel for moves
	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create publishing channel: %v", err)
	}
	defer pubCh.Close()

	// Subscribe to army moves from other players
	moveQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	moveRoutingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	err = pubsub.SubscribeJSON(
		conn,                                    // connection
		routing.ExchangePerilTopic,              // exchange (topic exchange)
		moveQueueName,                           // queue name (army_moves.username)
		moveRoutingKey,                          // routing key (army_moves.*)
		pubsub.Transient,                        // queue type
		handlerMove(gameState, pubCh, username), // handler function
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to army move messages: %v", err)
	}

	// Subscribe to war messages from all players
	warQueueName := routing.WarRecognitionsPrefix // Use shared "war" queue
	warRoutingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(
		conn,                                   // connection
		routing.ExchangePerilTopic,             // exchange (topic exchange)
		warQueueName,                           // queue name (war) - shared by all clients
		warRoutingKey,                          // routing key (war.*)
		pubsub.Durable,                         // queue type - should be durable for shared queue
		handlerWar(gameState, pubCh, username), // handler function
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to war messages: %v", err)
	}

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
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
			} else {
				fmt.Println("Move successful!")

				// Publish the move to other players
				movePublishKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
				err = pubsub.PublishJSON(pubCh, routing.ExchangePerilTopic, movePublishKey, move)
				if err != nil {
					log.Printf("Failed to publish move: %v", err)
				} else {
					fmt.Println("Move published successfully!")
				}
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
