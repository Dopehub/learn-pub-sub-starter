package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	cnx_string := "amqp://guest:guest@localhost:5672/"
	cnx, err := amqp.Dial(cnx_string)
	if err != nil {
		log.Fatalf("Could not connect to Rabbit MQ: %v", err)
	}
	defer cnx.Close()

	fmt.Println("Connection to the queue successful!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Something went wrong when retrieving username?: %v", err)
	}

	// creating the game state
	gameState := gamelogic.NewGameState(username)

	// creating the publish channel
	publishChan, err := cnx.Channel()

	// clients subscribe to the pause key sent from the server
	if err := pubsub.Subscribe(cnx,
		routing.ExchangePerilDirect,
		strings.Join([]string{routing.PauseKey, username}, "."),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
		pubsub.UnmarshallerJson,
	); err != nil {
		log.Fatalf("Something went wrong: %v", err)
	}
	// clients subscibe to other players move actions on a topic
	if err := pubsub.Subscribe(cnx,
		routing.ExchangePerilTopic,
		strings.Join([]string{"army_moves", username}, "."),
		"army_moves.*",
		pubsub.Transient,
		handlerMove(gameState, publishChan),
		pubsub.UnmarshallerJson,
	); err != nil {
		log.Fatalf("Something went wrong: %v", err)
	}

	// clients subscribe to the war messages
	if err := pubsub.Subscribe(cnx,
		routing.ExchangePerilTopic,
		"war",
		"war.*",
		pubsub.Durable,
		handlerWar(gameState, publishChan),
		pubsub.UnmarshallerGob,
	); err != nil {
		log.Fatalf("Something went wrong: %v", err)
	}

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
			if armyMV, err := gameState.CommandMove(userInput); err != nil {
				fmt.Printf("Error moving the unit: %v, try again: \n", err)
				continue
			} else {
				err = pubsub.PublishJSON(publishChan, routing.ExchangePerilTopic, "army_moves."+username, armyMV)
				if err != nil {
					log.Fatalf("could not publish message: %v", err)
				}
				fmt.Printf("Moving the selected army to: %v was successful\n", armyMV.ToLocation)
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

func gameLogConstruct(ch *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:    username,
			Message:     msg,
			CurrentTime: time.Now(),
		},
	)
}
