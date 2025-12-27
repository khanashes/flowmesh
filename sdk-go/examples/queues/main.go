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

	// Example 1: Enqueue jobs
	fmt.Println("Enqueueing jobs...")
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf(`{"job_id": %d, "task": "process_order"}`, i))
		jobID, seq, err := c.Queue.Enqueue(
			ctx,
			"default",
			"test",
			"orders",
			payload,
			client.WithHeaders(map[string]string{"priority": "high"}),
		)
		if err != nil {
			log.Fatalf("Failed to enqueue job: %v", err)
		}
		fmt.Printf("Enqueued job: ID=%s, Seq=%d\n", jobID, seq)
	}

	// Example 2: Get queue statistics
	fmt.Println("\nGetting queue statistics...")
	stats, err := c.Queue.GetStats(ctx, "default", "test", "orders")
	if err != nil {
		log.Fatalf("Failed to get queue stats: %v", err)
	}
	fmt.Printf("Queue Stats:\n")
	fmt.Printf("  Total Jobs: %d\n", stats.TotalJobs)
	fmt.Printf("  Pending Jobs: %d\n", stats.PendingJobs)
	fmt.Printf("  In-Flight Jobs: %d\n", stats.InFlightJobs)
	fmt.Printf("  Completed Jobs: %d\n", stats.CompletedJobs)

	// Example 3: Reserve a single job
	fmt.Println("\nReserving a job...")
	job, err := c.Queue.Reserve(
		ctx,
		"default",
		"test",
		"orders",
		client.WithVisibilityTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to reserve job: %v", err)
	}
	if job != nil {
		fmt.Printf("Reserved job: ID=%s, Payload=%s\n", job.ID, string(job.Payload))
		// Process job...
		time.Sleep(100 * time.Millisecond)
		// Acknowledge job
		if err := c.Queue.ACK(ctx, "default", "test", "orders", job.ID); err != nil {
			log.Fatalf("Failed to ACK job: %v", err)
		}
		fmt.Println("Job acknowledged")
	} else {
		fmt.Println("No jobs available")
	}

	// Example 4: Receive multiple jobs (batch receive)
	fmt.Println("\nReceiving multiple jobs...")
	jobs, err := c.Queue.Receive(
		ctx,
		"default",
		"test",
		"orders",
		client.WithMaxJobs(3),
		client.WithReceiveVisibilityTimeout(30*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to receive jobs: %v", err)
	}
	fmt.Printf("Received %d jobs\n", len(jobs))
	for _, job := range jobs {
		fmt.Printf("  Job ID: %s, Payload: %s\n", job.ID, string(job.Payload))
		// Process job...
		if err := c.Queue.ACK(ctx, "default", "test", "orders", job.ID); err != nil {
			log.Printf("Failed to ACK job %s: %v", job.ID, err)
		}
	}

	// Example 5: Worker pattern (simple worker loop)
	fmt.Println("\nRunning worker pattern...")
	go func() {
		for {
			job, err := c.Queue.Reserve(
				ctx,
				"default",
				"test",
				"orders",
				client.WithVisibilityTimeout(30*time.Second),
				client.WithLongPollTimeout(5*time.Second), // Long poll for up to 5 seconds
			)
			if err != nil {
				log.Printf("Failed to reserve job: %v", err)
				time.Sleep(time.Second)
				continue
			}

			if job == nil {
				// No jobs available, continue
				continue
			}

			// Process job
			fmt.Printf("Processing job: %s\n", job.ID)
			// Simulate processing
			time.Sleep(100 * time.Millisecond)

			// Acknowledge job
			if err := c.Queue.ACK(ctx, "default", "test", "orders", job.ID); err != nil {
				log.Printf("Failed to ACK job: %v", err)
				// Could NACK instead if needed
				_ = c.Queue.NACK(ctx, "default", "test", "orders", job.ID)
			}
		}
	}()

	// Run worker for a short time
	time.Sleep(2 * time.Second)

	fmt.Println("\nQueue examples completed!")
}
