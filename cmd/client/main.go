package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	cnx_string := "amqp://guest:guest@localhost:5672/"
	cnx, err := amqp091.Dial(cnx_string)
	if err != nil {
		log.Fatalf("Could not connect to Rabbit MQ: %v", err)
	}
	defer cnx.Close()

	fmt.Println("Connection to the queue successful!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Something went wrong when retrieving username?: %v", err)
	}

	_, queue, err := pubsub.DeclareAndBind(cnx,
		routing.ExchangePerilDirect,
		strings.Join([]string{routing.PauseKey, username}, "."),
		routing.PauseKey,
		pubsub.Transient)

	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gameState := gamelogic.NewGameState(username)

	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0] {
		case "spawn":
			if err := gameState.CommandSpawn(userInput); err != nil {
				fmt.Printf("Error spawning a new unit: %v\n, try again: \n", err)
				continue
			}
		case "move":
			if army, err := gameState.CommandMove(userInput); err != nil {
				fmt.Printf("Error moving the unit: %v, try again: \n", err)
				continue
			} else {
				fmt.Printf("Moving the selected army to: %v was successful\n", army.ToLocation)
			}
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Command not known to game...")
			continue
		}
	}

	// // wait for exit Ctr c
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan

}
