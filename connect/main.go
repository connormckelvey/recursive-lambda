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

func s3Worker(id int, messages <-chan *kafka.Message, consumer *kafka.Consumer, quit chan struct{}) {
	for {
		select {
		case <-quit:
			fmt.Println("Quit signal received, working", id, "signing off")
			return
		case msg := <-messages:
			fmt.Println("Worker", id, "uploading", msg)
			time.Sleep(1 * time.Second) //Upload to s3 here
			fmt.Println("Worker", id, "uploaded", msg)
			if _, err := consumer.CommitMessage(msg); err != nil {
				fmt.Println("Err", err)
			} else {
				fmt.Println("Worker", id, "commited offset for", msg)
			}
		}
	}
}

func startWorkers(consumer *kafka.Consumer, messages chan *kafka.Message, quit chan struct{}) {
	fmt.Println("Waiting to save messages...")
	workers := 50
	for w := 0; w < workers; w++ {
		go s3Worker(w, messages, consumer, quit)
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
	go startWorkers(consumer, messages, quit)

	for {
		select {
		case <-quit:
			consumer.Close()
			// Recurse
			// lambda.Invoke()
			return nil
		case <-ctx.Done():
			fmt.Println("Done Channel Called-- Lambda Shutting Down")
			fmt.Println("Number of unprocessed Messages", len(messages))
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	Handler(ctx)
	// lambda.Start(Handler)
	cancel()
}
