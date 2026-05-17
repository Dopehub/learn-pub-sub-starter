package main

import (
	"fmt"
	"log"

	// "os"
	// "os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	fmt.Println("Starting Peril server...")

	cnx_string := "amqp://guest:guest@localhost:5672/"
	cnx, err := amqp.Dial(cnx_string)
	if err != nil {
		log.Fatalf("Could not connect to Rabbit MQ: %v", err)
	}
	defer cnx.Close()

	fmt.Println("Connection to the queue successful!")

	_, queue, err := pubsub.DeclareAndBind(cnx,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		})

	pubsub.Subscribe(cnx,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.Durable,
		handleLogs(),
		pubsub.UnmarshallerGob,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	// creating the publish channel
	publishChan, err := cnx.Channel()

	if err != nil {
		log.Fatalf("Connection to the channel unsuccessful, could not create channel %v", err)
	}

	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		switch userInput[0] {
		case "pause":
			fmt.Println("Sending a pause message to queue")

			err = pubsub.PublishJSON(publishChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})

			if err != nil {
				log.Fatalf("could not publish message: %v", err)
			}
		case "resume":
			fmt.Println("Sending a resume message to queue")

			err = pubsub.PublishJSON(publishChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})

			if err != nil {
				log.Fatalf("could not publish message: %v", err)
			}
		case "quit":
			fmt.Println("Quitting the game...")
			return
		default:
			fmt.Println("Command not known to game")
		}
	}

}
