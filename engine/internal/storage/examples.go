package storage

import (
	"context"
	"fmt"
	"time"
)

// ExampleUsage demonstrates how to use the storage framework
func ExampleUsage() {
	ctx := context.Background()

	// Example 1: Using the builder pattern
	storage, err := NewBuilder().
		WithDataDir("./data").
		BuildAndStart(ctx)
	if err != nil {
		panic(err)
	}
	defer storage.Stop(ctx)

	// Example 2: Using the factory pattern to create resources
	factory := NewResourceFactory(storage.MetaStore())

	// Create a stream
	stream, err := factory.CreateStream(ctx, "tenant1", "ns1", "my-stream", 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created stream: %s\n", stream.GetPath())

	// Create a queue
	queue, err := factory.CreateQueue(ctx, "tenant1", "ns1", "my-queue", 1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created queue: %s\n", queue.GetPath())

	// Example 3: Using the stream manager
	streamMgr := storage.StreamManager()

	// Write events
	events := []StreamEvent{
		{Payload: []byte("event1"), Headers: map[string]string{"key": "value"}},
		{Payload: []byte("event2"), Headers: nil},
	}
	offsets, err := streamMgr.WriteEvents(ctx, stream.GetPath(), events)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Written events at offsets: %v\n", offsets)

	// Read events
	messages, err := streamMgr.ReadFromOffset(ctx, stream.GetPath(), 0, 0, 10)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read %d messages\n", len(messages))

	// Example 4: Using the queue manager
	queueMgr := storage.QueueManager()

	// Enqueue a job
	jobID, seq, err := queueMgr.Enqueue(ctx, queue.GetPath(), []byte("job payload"), QueueEnqueueOptions{
		Delay:   0,
		Headers: map[string]string{"priority": "high"},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Enqueued job: %s (seq: %d)\n", jobID, seq)

	// Reserve a job
	job, err := queueMgr.Reserve(ctx, queue.GetPath(), 30*time.Second)
	if err != nil {
		panic(err)
	}
	if job != nil {
		fmt.Printf("Reserved job: %s\n", job.ID)

		// Process job...

		// Acknowledge job
		err = queueMgr.RemoveFromInFlight(ctx, queue.GetPath(), job.ID)
		if err != nil {
			panic(err)
		}
	}

	// Example 5: Using the KV manager
	kvMgr := storage.KVManager()

	// Create a KV store
	kvStore, err := factory.CreateKVStore(ctx, "tenant1", "ns1", "my-kv")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created KV store: %s\n", kvStore.GetPath())

	// Initialize KV store
	err = kvMgr.InitializeKVStore(ctx, kvStore.GetPath())
	if err != nil {
		panic(err)
	}

	// Set a value
	err = kvMgr.Set(ctx, kvStore.GetPath(), "user:123", []byte(`{"name":"John","age":30}`), KVSetOptions{
		TTL: 3600 * time.Second, // 1 hour TTL
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Set key: user:123\n")

	// Get a value
	value, err := kvMgr.Get(ctx, kvStore.GetPath(), "user:123")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Got value: %s\n", string(value))

	// Check if key exists
	exists, err := kvMgr.Exists(ctx, kvStore.GetPath(), "user:123")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Key exists: %v\n", exists)

	// List keys with prefix
	keys, err := kvMgr.ListKeys(ctx, kvStore.GetPath(), "user:")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Keys with prefix 'user:': %v\n", keys)

	// Delete a key
	err = kvMgr.Delete(ctx, kvStore.GetPath(), "user:123")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Deleted key: user:123\n")
}
