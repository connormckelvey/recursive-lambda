package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func receiveMessages(consumer *kafka.Consumer, messages chan *kafka.Message, quit chan struct{}) {
	fmt.Println("Waiting to consume messages...")
	for {
		msg, err := consumer.ReadMessage(-1) // Check for timeout error, if so maybe dont recurse?
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
	fmt.Println("Waiting to save messages...")
	for {
		select {
		case msg := <-messages:
			go saveMessage(consumer, msg)
		}
	}
}

func timeRemaining(ctx context.Context) float64 {
	deadline, _ := ctx.Deadline()
	return time.Until(deadline).Seconds() / 1000
}

func receiveDeadline(ctx context.Context, quit chan struct{}) {
	fmt.Println("Waiting for Deadline...")
	for {
		if timeRemaining(ctx) <= 1000 {
			close(quit)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var config = &kafka.ConfigMap{
	"bootstrap.servers": "localhost",
	"group.id":          "myGroup",
	"auto.offset.reset": "earliest",
}
var topics = []string{"myTopic", "^aRegex$"}

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
	messages := make(chan *kafka.Message, 50) // Buffered channels will block, this way we dont consume memory with messages before we can save them, option maybe?

	go receiveDeadline(ctx, quit)
	go receiveMessages(consumer, messages, quit)
	go saveMessages(consumer, messages, quit)

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
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	Handler(ctx)
	// lambda.Start(Handler)
	cancel()
}
