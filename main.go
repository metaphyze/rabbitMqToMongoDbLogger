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

type LogEvent struct {
	Username       string  `json:"username"`
	Problem        string  `json:"problem"`
	ID             string  `json:"id"`
	Server         string  `json:"server"`
	RequestNum     uint64  `json:"request_num"`
	StartTime      string  `json:"start_time"`
	StartTimeMs    int64   `json:"start_time_ms"`
	DurationMs     int64   `json:"duration_ms"`
	Success        bool    `json:"success"`
	Error          string  `json:"error"`
	Answer         float64 `json:"answer"`
	HTTPReturnCode int     `json:"http_return_code"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func connectToMongo(mongoHost string, databaseName string, collectionName string) *mongo.Collection {
	// MongoDB connection
	clientOptions := options.Client().ApplyURI(fmt.Sprintf("mongodb://%v:27017", mongoHost))
	client, err := mongo.Connect(context.TODO(), clientOptions)
	failOnError(err, "Failed to connect to MongoDB")

	// Ensure MongoDB connection is alive
	err = client.Ping(context.TODO(), nil)
	failOnError(err, "Failed to ping MongoDB")

	fmt.Println("Connected to MongoDB")
	collection := client.Database(databaseName).Collection(collectionName)
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
	rabbitMQUserName := os.Getenv("RABBITMQ_USERNAME")
	rabbitMQPassword := os.Getenv("RABBITMQ_PASSWORD")

	// Connect to RabbitMQ
	connectionStr := fmt.Sprintf("amqp://%v:%v@%v:5672/", rabbitMQUserName, rabbitMQPassword, rabbitMQHost)
	conn, err := amqp.Dial(connectionStr)
	failOnError(err, fmt.Sprintf("Failed to connect to RabbitMQ (%v)", connectionStr))
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare queues
	userEventsQueue, err := ch.QueueDeclare(
		"user_events", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare user_events queue")

	calculationEventsQueue, err := ch.QueueDeclare(
		"calculation_events", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare calculation_events queue")

	// Connect to MongoDB
	collectionUserEvents := connectToMongo(mongoDBHost, "user_events", "events")
	collectionCalculationEvents := connectToMongo(mongoDBHost, "calculation_events", "events")

	// Consume messages from user_events queue
	userEventsMsgs, err := ch.Consume(
		userEventsQueue.Name, // queue
		"",                   // consumer
		true,                 // auto-ack
		false,                // exclusive
		false,                // no-local
		false,                // no-wait
		nil,                  // args
	)
	failOnError(err, "Failed to register a consumer for user_events")

	// Consume messages from calculation_events queue
	calculationEventsMsgs, err := ch.Consume(
		calculationEventsQueue.Name, // queue
		"",                          // consumer
		true,                        // auto-ack
		false,                       // exclusive
		false,                       // no-local
		false,                       // no-wait
		nil,                         // args
	)
	failOnError(err, "Failed to register a consumer for calculation_events")

	forever := make(chan bool)

	go func() {
		for d := range userEventsMsgs {
			var event Event
			err := json.Unmarshal(d.Body, &event)
			if err != nil {
				log.Printf("Failed to unmarshal message from user_events: %s", err)
				continue
			}

			// Convert timestamp to milliseconds since epoch
			event.TimestampMs = convertToMilliseconds(event.Timestamp)

			// Save event to MongoDB
			_, err = collectionUserEvents.InsertOne(context.TODO(), bson.M{
				"username":     event.Username,
				"event_type":   event.EventType,
				"timestamp":    event.Timestamp,
				"timestamp_ms": event.TimestampMs,
				"target_user":  event.TargetUser,
				"ip_address":   event.IpAddress,
			})
			if err != nil {
				log.Printf("Failed to insert user event (%+v) into MongoDB: %s", event, err)
			} else {
				log.Printf("User event saved: %+v", event)
			}
		}
	}()

	go func() {
		for d := range calculationEventsMsgs {
			var logEvent LogEvent
			err := json.Unmarshal(d.Body, &logEvent)
			if err != nil {
				log.Printf("Failed to unmarshal message from calculation_events: %s", err)
				continue
			}

			// Save logEvent to MongoDB
			_, err = collectionCalculationEvents.InsertOne(context.TODO(), bson.M{
				"username":         logEvent.Username,
				"problem":          logEvent.Problem,
				"id":               logEvent.ID,
				"server":           logEvent.Server,
				"request_num":      logEvent.RequestNum,
				"start_time":       logEvent.StartTime,
				"start_time_ms":    logEvent.StartTimeMs,
				"duration_ms":      logEvent.DurationMs,
				"success":          logEvent.Success,
				"error":            logEvent.Error,
				"answer":           logEvent.Answer,
				"http_return_code": logEvent.HTTPReturnCode,
			})
			if err != nil {
				log.Printf("Failed to insert calculation event (%+v) into MongoDB: %s", logEvent, err)
			} else {
				log.Printf("Calculation event saved: %+v", logEvent)
			}
		}
	}()

	log.Printf("Waiting for messages")
	<-forever
}
