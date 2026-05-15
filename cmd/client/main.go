package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
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

	pubsub.DeclareAndBind(cnx,
		routing.ExchangePerilDirect,
		strings.Join([]string{routing.PauseKey, username}, "."),
		routing.PauseKey,
		pubsub.Transient)

	// wait for exit Ctr c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

}
