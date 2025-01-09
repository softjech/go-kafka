package main

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	app.Listen(":3000")
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return producer, nil
}

func PushCommentToQueue(topic string, comment []byte) {
	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Println(err)
		return
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(comment),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

}

func createComment(c *fiber.Ctx) error {
	comment := new(Comment)
	if err := c.BodyParser(comment); err != nil {
		log.Println(err)
		c.Status(400).JSON(fiber.Map{
			"succes":  false,
			"message": err,
		})
		return err
	}
	cmtInBytes, err := json.Marshal(comment)
	if err != nil {
		log.Println(err)
		c.Status(500).JSON(fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}
	PushCommentToQueue("comments", cmtInBytes)
	return c.JSON(fiber.Map{
		"success": true,
		"message": "Comment created successfully",
	})
}
