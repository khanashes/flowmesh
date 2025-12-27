package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/flowmesh/sdk-go/client"
)

func main() {
	// Create client
	c, err := client.NewClient("localhost:50051")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// Example 1: Write events to a stream
	fmt.Println("Writing events to stream...")
	events := []client.Event{
		{
			Payload: []byte(`{"order_id": "123", "amount": 99.99}`),
			Headers: map[string]string{"event_type": "order.created"},
		},
		{
			Payload: []byte(`{"order_id": "124", "amount": 149.99}`),
			Headers: map[string]string{"event_type": "order.created"},
		},
	}

	offsets, err := c.Stream.Write(ctx, "default", "test", "orders", events)
	if err != nil {
		log.Fatalf("Failed to write events: %v", err)
	}
	fmt.Printf("Events written at offsets: %v\n", offsets)

	// Example 2: Read messages from a stream
	fmt.Println("\nReading messages from stream...")
	messages, err := c.Stream.Read(ctx, "default", "test", "orders", 0, 0, 10)
	if err != nil {
		log.Fatalf("Failed to read messages: %v", err)
	}
	fmt.Printf("Read %d messages\n", len(messages))
	for _, msg := range messages {
		fmt.Printf("  Offset: %d, Payload: %s\n", msg.Offset, string(msg.Payload))
	}

	// Example 3: Subscribe to a stream with consumer group
	fmt.Println("\nSubscribing to stream with consumer group...")
	msgChan, errChan := c.Stream.Subscribe(
		ctx,
		"default",
		"test",
		"orders",
		"billing-service",
		client.WithStartOffset(-2), // Start from latest
	)

	// Start a goroutine to handle messages
	go func() {
		for msg := range msgChan {
			fmt.Printf("Received message: Offset=%d, Payload=%s\n", msg.Offset, string(msg.Payload))
			// Commit offset after processing
			if err := c.Stream.CommitOffset(ctx, "default", "test", "orders", "billing-service", 0, msg.Offset); err != nil {
				log.Printf("Failed to commit offset: %v", err)
			}
		}
	}()

	// Handle errors
	go func() {
		for err := range errChan {
			log.Printf("Subscription error: %v", err)
		}
	}()

	// Run for a short time to demonstrate
	time.Sleep(2 * time.Second)

	// Example 4: Get consumer group state
	fmt.Println("\nGetting consumer group state...")
	state, err := c.Stream.GetConsumerGroupState(ctx, "default", "test", "orders", "billing-service", 0)
	if err != nil {
		log.Fatalf("Failed to get consumer group state: %v", err)
	}
	fmt.Printf("Consumer Group State:\n")
	fmt.Printf("  Committed Offset: %d\n", state.CommittedOffset)
	fmt.Printf("  Latest Offset: %d\n", state.LatestOffset)
	fmt.Printf("  Lag: %d\n", state.Lag)

	fmt.Println("\nStream examples completed!")
}
