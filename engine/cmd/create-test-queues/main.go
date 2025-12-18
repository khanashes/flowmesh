package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
)

func main() {
	// Use default data directory (same as running server)
	dataDir := "./data"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}

	ctx := context.Background()

	// Build storage
	storageBackend, err := storage.NewBuilder().
		WithDataDir(dataDir).
		Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build storage: %v\n", err)
		os.Exit(1)
	}
	defer storageBackend.Close(ctx)

	// Create test queues
	queues := []struct {
		tenant     string
		namespace  string
		name       string
		partitions int
	}{
		{"default", "test", "test-queue", 1},
		{"default", "test", "priority-queue", 1},
		{"production", "orders", "order-processing", 1},
	}

	metaStore := storageBackend.MetaStore()
	queueMgr := storageBackend.QueueManager()

	fmt.Println("Creating queues...")
	for _, q := range queues {
		config := &metastore.ResourceConfig{
			Tenant:     q.tenant,
			Namespace:  q.namespace,
			Name:       q.name,
			Type:       metastore.ResourceQueue,
			Partitions: q.partitions,
			CreatedAt:  time.Now(),
		}

		if err := metaStore.CreateResource(config); err != nil {
			if _, ok := err.(metastore.ResourceExistsError); ok {
				fmt.Printf("Queue already exists: %s/%s/%s\n", q.tenant, q.namespace, q.name)
			} else {
				fmt.Fprintf(os.Stderr, "Failed to create queue %s/%s/%s: %v\n", q.tenant, q.namespace, q.name, err)
				continue
			}
		} else {
			fmt.Printf("Created queue: %s/%s/%s\n", q.tenant, q.namespace, q.name)
		}

		// Initialize the queue
		resourcePath := fmt.Sprintf("%s/%s/queue/%s", q.tenant, q.namespace, q.name)
		if err := queueMgr.InitializeQueue(ctx, resourcePath); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize queue %s: %v\n", resourcePath, err)
			continue
		}

		// Add some test jobs
		if q.tenant == "default" && q.namespace == "test" && q.name == "test-queue" {
			fmt.Printf("Adding test jobs to %s...\n", resourcePath)
			for i := 1; i <= 5; i++ {
				payload := []byte(fmt.Sprintf("Test job %d", i))
				jobID, seq, err := queueMgr.Enqueue(ctx, resourcePath, payload, storage.QueueEnqueueOptions{
					Headers: map[string]string{
						"job-number": fmt.Sprintf("%d", i),
						"source":     "test-script",
					},
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to enqueue job %d: %v\n", i, err)
				} else {
					fmt.Printf("  Enqueued job %d: ID=%s, Seq=%d\n", i, jobID, seq)
				}
			}
		}
	}

	fmt.Println("\nDone! You can now view the queues in the Web UI at http://localhost:8080/queues")
}
