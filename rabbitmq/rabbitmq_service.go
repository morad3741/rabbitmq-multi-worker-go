package rabbitmq

import (
	"fmt"
	"log"

	"github.com/wagslane/go-rabbitmq"
)

// RabbitMQService handles RabbitMQ interactions
type RabbitMQService struct {
	conn             *rabbitmq.Conn
	producerChannels map[string]chan string // Store channels for each queueName
}

// NewRabbitMQService initializes a new RabbitMQ service
func NewRabbitMQService(amqpURL string) (*RabbitMQService, error) {
	conn, err := rabbitmq.NewConn(
		amqpURL,
		rabbitmq.WithConnectionOptionsLogging, // Enable logging
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	return &RabbitMQService{
		conn:             conn,
		producerChannels: make(map[string]chan string), // Initialize the map
	}, nil
}

// consumeMessage consumes messages from the specified queue and processes them
func (r *RabbitMQService) Consume(queueName string, numConsumerThreads int, processFunc func(event string)) error {

	consumer, err := rabbitmq.NewConsumer(
		r.conn,
		queueName, // Queue name
		rabbitmq.WithConsumerOptionsConcurrency(numConsumerThreads), // Set concurrency level
		rabbitmq.WithConsumerOptionsQueueAutoDelete,
		rabbitmq.WithConsumerOptionsQOSPrefetch(0),
	)
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	log.Printf("Consuming from queue: %s with %d concurrent threads", queueName, numConsumerThreads)

	// Start consuming messages
	go func() {
		err := consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			processFunc(string(d.Body)) // Call the processing function
			return rabbitmq.Ack         // Acknowledge message
		})
		if err != nil {
			log.Printf("Error in consuming messages: %v", err)
		}
	}()

	return nil
}

// DefineProducers initializes multiple producer goroutines that wait for messages to send
func (r *RabbitMQService) DefineProducers(queueName string, numProducerThreads int) error {
	// Check if producers for the queue already exist
	if _, exists := r.producerChannels[queueName]; exists {
		log.Printf("Producers for queue %s are already defined", queueName)
		return nil
	}

	// Create a message channel for this queue
	messageChannel := make(chan string, 100) // Buffered channel for messages

	// Store the channel in the producerChannels map
	r.producerChannels[queueName] = messageChannel

	// Start multiple producer goroutines
	for i := 0; i < numProducerThreads; i++ {
		fmt.Printf("Starting producer thread number %d\n", i)
		go func(i int) {
			// Create a new publisher for each producer goroutine
			publisher, err := rabbitmq.NewPublisher(
				r.conn,
				rabbitmq.WithPublisherOptionsExchangeName(""),
			)
			if err != nil {
				log.Printf("Failed to create publisher for producer #%d: %v", i, err)
				return
			}
			defer publisher.Close()

			// Each goroutine listens for a message to send and sends it to the queue
			for message := range messageChannel {
				// Send the message to the queue
				err := publisher.Publish(
					[]byte(message),     // Message body as []byte
					[]string{queueName}, // Routing key as []string (single queue name in the list)
				)
				if err != nil {
					log.Printf("Failed to publish message from producer #%d: %v", i, err)
				} else {
					log.Printf("Producer #%d published message: %s", i, message)
				}
			}
		}(i)
	}

	return nil
}

// SendMessage puts a message into the channel for the respective queue
func (r *RabbitMQService) SendMessage(queueName string, message string) error {
	// Check if the channel for the queue exists
	messageChannel, exists := r.producerChannels[queueName]
	if !exists {
		return fmt.Errorf("no producers defined for queue %s", queueName)
	}

	// Put the message into the channel for producers to pick up
	messageChannel <- message
	log.Printf("Message for queue %s added to channel: %s", queueName, message)
	return nil
}

// Close closes the RabbitMQ connection
func (r *RabbitMQService) Close() {
	r.conn.Close()
}
