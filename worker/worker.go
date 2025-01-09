package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Worker started")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0
	doneChan := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received messages", string(msg.Value))
				fmt.Println("Total messages received", msgCount)
				fmt.Println("Topic", msg.Topic)
			case <-signalChan:
				fmt.Println("Interrupt is detected")
				doneChan <- struct{}{}
			}
		}
	}()
	<-doneChan
	fmt.Println("Processed", msgCount, "messages")
	if err := consumer.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
// --header 'Content-Type: application/json' \
// --data-raw '{ "text":"message 1" }'
// {"message":"Comment created successfully","success":true}%
// surajjha@MacBook-Pro go-kafka % curl --location --request POST '0.0.0.0:3000/api/v1/comments' \
// --header 'Content-Type: application/json' \
// --data-raw '{ "text":"message 2" }'
// {"message":"Comment created successfully","success":true}%
