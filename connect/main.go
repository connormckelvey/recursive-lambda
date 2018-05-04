package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var config = &kafka.ConfigMap{
	"bootstrap.servers": "localhost",
	"group.id":          "myGroup",
	"auto.offset.reset": "earliest",
}
var topics = []string{"myTopic", "^aRegex$"}

func receiveMessages(consumer *kafka.Consumer, messages chan *kafka.Message, quit chan struct{}) {
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			return
		}
		messages <- msg
		select {
		case <-quit:
			return
		}
	}
}

func saveMessage(consumer *kafka.Consumer, message *kafka.Message) {
	// Upload to S3
	consumer.CommitMessage(message)
}

func saveMessages(consumer *kafka.Consumer, messages chan *kafka.Message, quit chan struct{}) {
	for {
		select {
		case msg := <-messages:
			go saveMessage(consumer, msg)
		}
	}
}

func receiveDeadline(ctx context.Context, quit chan struct{}) {
	for {
		deadline, _ := ctx.Deadline()
		timeRemaining := time.Until(deadline).Truncate(100 * time.Millisecond)
		fmt.Printf("Time Remaining %d", timeRemaining)
		if timeRemaining <= 0 {
			close(quit)
			return
		}
	}
}

func Handler(ctx context.Context) error {
	fmt.Println("Connecting to Kafka")
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatal(err)
		return err
	}
	fmt.Println("Connected")

	fmt.Println("Subscribing to topics")
	consumer.SubscribeTopics(topics, nil)
	fmt.Println("Subscribed")

	quit := make(chan struct{})
	messages := make(chan *kafka.Message)

	go receiveMessages(consumer, messages, quit)
	go saveMessages(consumer, messages, quit)
	go receiveDeadline(ctx, quit)

	for {
		select {
		case <-quit:
			consumer.Close()
			// Recurse
			// lambda.Invoke()
			return nil
		}
	}
}

func main() {
	Handler(context.Background())
	// lambda.Start(Handler)
}
