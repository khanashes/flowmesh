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
	defer func() {
		//nolint:errcheck // Ignore stop errors in example code
		_ = storage.Stop(ctx)
	}()

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

// ExampleConsumerGroups demonstrates consumer group operations
func ExampleConsumerGroups() {
	ctx := context.Background()

	// Create storage
	storage, err := New("data")
	if err != nil {
		panic(err)
	}
	defer storage.Close(ctx)

	if err := storage.Start(ctx); err != nil {
		panic(err)
	}

	// Create a stream first
	factory := NewResourceFactory(storage.MetaStore())
	stream, err := factory.CreateStream(ctx, "tenant1", "ns1", "events", 1)
	if err != nil {
		panic(err)
	}

	streamMgr := storage.StreamManager()
	consumerGroupMgr := storage.ConsumerGroupManager()

	// Write some events to the stream
	events := []StreamEvent{
		{Payload: []byte("event1")},
		{Payload: []byte("event2")},
		{Payload: []byte("event3")},
	}
	offsets, err := streamMgr.WriteEvents(ctx, stream.GetPath(), events)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Wrote %d events, last offset: %d\n", len(events), offsets[len(offsets)-1])

	// Create a consumer group and commit offset
	streamPath := stream.GetPath()
	groupName := "my-consumer-group"
	partition := int32(0)

	// Commit offset after reading first event (offset 0)
	err = consumerGroupMgr.CommitOffset(ctx, streamPath, groupName, partition, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Committed offset 0 for consumer group '%s'\n", groupName)

	// Get the committed offset
	committedOffset, err := consumerGroupMgr.GetCommittedOffset(ctx, streamPath, groupName, partition)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Committed offset: %d\n", committedOffset)

	// Calculate consumer lag
	lag, err := consumerGroupMgr.CalculateLag(ctx, streamPath, groupName, partition)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Consumer lag: %d\n", lag)

	// Get full consumer group state
	state, err := consumerGroupMgr.GetConsumerGroupState(ctx, streamPath, groupName, partition)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Consumer group state:\n")
	fmt.Printf("  Stream: %s\n", state.Stream)
	fmt.Printf("  Group: %s\n", state.Group)
	fmt.Printf("  Committed Offset: %d\n", state.CommittedOffset)
	fmt.Printf("  Latest Offset: %d\n", state.LatestOffset)
	fmt.Printf("  Lag: %d\n", state.Lag)

	// List all consumer groups for the stream
	groups, err := consumerGroupMgr.ListConsumerGroups(ctx, streamPath)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Consumer groups for stream: %v\n", groups)

	// Commit a new offset (after reading more events)
	err = consumerGroupMgr.CommitOffset(ctx, streamPath, groupName, partition, 2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Updated committed offset to 2\n")

	// Check lag again
	lag, err = consumerGroupMgr.CalculateLag(ctx, streamPath, groupName, partition)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Updated consumer lag: %d\n", lag)
}
