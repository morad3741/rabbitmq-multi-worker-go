package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"rabbitMq-go/rabbitmq"
)

func main() {
	// Initialize the RabbitMQService
	rmqService, err := rabbitmq.NewRabbitMQService("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Error initializing RabbitMQ service: %v", err)
	}
	defer rmqService.Close()

	//Define producers (if needed)
	err = rmqService.DefineProducers("queue1", 2)
	if err != nil {
		log.Printf("Error defining producers: %v", err)
	}

	//Start consuming messages
	err = rmqService.Consume("queue1", 2, processMessage)
	if err != nil {
		log.Printf("Error in consuming messages: %v", err)
	}

	// Start HTTP server for publishing messages
	http.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		// Call SendMessage here
		queueName := "queue1"      // Example queue name
		message := "Hello, World!" // Example message

		err := rmqService.SendMessage(queueName, message)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Message sent"))
	})

	// Start HTTP server
	log.Println("HTTP server is listening on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// processMessage simulates business logic and sleeps for 10 seconds
func processMessage(event string) {
	log.Printf("Processing message: %s", event)
	time.Sleep(10 * time.Second) // Simulate 10 seconds of processing time
	log.Printf("Finished processing message: %s", event)
}
