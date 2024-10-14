package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Event struct {
	Username    string `json:"username"`
	EventType   string `json:"event_type"`
	Timestamp   string `json:"timestamp"`
	TimestampMs int64  `json:"timestamp_ms"`
	TargetUser  string `json:"target_user"`
	IpAddress   string `json:"ip_address"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func connectToMongo(mongoHost string) *mongo.Collection {
	// MongoDB connection
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%v:27017", mongoHost))
	client, err := mongo.Connect(context.TODO(), clientOptions)
	failOnError(err, "Failed to connect to MongoDB")

	// Ensure MongoDB connection is alive
	err = client.Ping(context.TODO(), nil)
	failOnError(err, "Failed to ping MongoDB")

	fmt.Println("Connected to MongoDB")
	collection := client.Database("user_events").Collection("events")
	return collection
}

func convertToMilliseconds(timestamp string) int64 {
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		log.Printf("Failed to parse timestamp: %s", err)
		return 0
	}
	return t.UnixNano() / int64(time.Millisecond)
}

func main() {
	rabbitMQHost := os.Getenv("RABBITMQ_HOST")
	mongoDBHost := os.Getenv("MONGODB_HOST")
	// Connect to RabbitMQ
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%v:5672/", rabbitMQHost))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare queue
	q, err := ch.QueueDeclare(
		"user_events", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Connect to MongoDB
	collection := connectToMongo(mongoDBHost)

	// Consume messages from RabbitMQ
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var event Event
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.Printf("Failed to unmarshal message: %s", err)
				continue
			}

			// Convert timestamp to milliseconds since epoch
			event.TimestampMs = convertToMilliseconds(event.Timestamp)

			// Save event to MongoDB
			_, err = collection.InsertOne(context.TODO(), bson.M{
				"username":     event.Username,
				"event_type":   event.EventType,
				"timestamp":    event.Timestamp,
				"timestamp_ms": event.TimestampMs,
				"target_user":  event.TargetUser,
				"ip_address":   event.IpAddress,
			})
			if err != nil {
				log.Printf("Failed to insert event (%+v) into MongoDB: %s", event, err)
			} else {
				//log.Printf("Event saved: %+v", event)
			}
		}
	}()

	log.Printf("Waiting for messages")
	<-forever
}
