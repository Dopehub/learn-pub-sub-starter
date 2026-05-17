package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handleLogs() func(routing.GameLog) pubsub.Acktype {
	return func(r routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		if err := gamelogic.WriteLog(r); err !=nil {
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}
