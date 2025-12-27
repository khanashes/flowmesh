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

	// Example 1: Set a value without TTL
	fmt.Println("Setting a value...")
	err = c.KV.Set(ctx, "default", "test", "cache", "user:123", []byte(`{"name": "John Doe", "email": "john@example.com"}`))
	if err != nil {
		log.Fatalf("Failed to set value: %v", err)
	}
	fmt.Println("Value set successfully")

	// Example 2: Get a value
	fmt.Println("\nGetting a value...")
	value, err := c.KV.Get(ctx, "default", "test", "cache", "user:123")
	if err != nil {
		log.Fatalf("Failed to get value: %v", err)
	}
	fmt.Printf("Retrieved value: %s\n", string(value))

	// Example 3: Set a value with TTL
	fmt.Println("\nSetting a value with TTL...")
	err = c.KV.Set(
		ctx,
		"default",
		"test",
		"cache",
		"session:abc123",
		[]byte(`{"user_id": "123", "session_token": "xyz"}`),
		client.WithTTL(1*time.Hour),
	)
	if err != nil {
		log.Fatalf("Failed to set value with TTL: %v", err)
	}
	fmt.Println("Value set with 1 hour TTL")

	// Example 4: Check if key exists
	fmt.Println("\nChecking if key exists...")
	exists, err := c.KV.Exists(ctx, "default", "test", "cache", "user:123")
	if err != nil {
		log.Fatalf("Failed to check existence: %v", err)
	}
	fmt.Printf("Key 'user:123' exists: %v\n", exists)

	// Example 5: List keys with prefix
	fmt.Println("\nListing keys with prefix...")
	// Set a few more keys
	c.KV.Set(ctx, "default", "test", "cache", "user:456", []byte(`{"name": "Jane Doe"}`))
	c.KV.Set(ctx, "default", "test", "cache", "user:789", []byte(`{"name": "Bob Smith"}`))
	c.KV.Set(ctx, "default", "test", "cache", "order:123", []byte(`{"order_id": "123"}`))

	keys, err := c.KV.ListKeys(ctx, "default", "test", "cache", "user:")
	if err != nil {
		log.Fatalf("Failed to list keys: %v", err)
	}
	fmt.Printf("Keys with prefix 'user:': %v\n", keys)

	// List all keys
	allKeys, err := c.KV.ListKeys(ctx, "default", "test", "cache", "")
	if err != nil {
		log.Fatalf("Failed to list all keys: %v", err)
	}
	fmt.Printf("All keys: %v\n", allKeys)

	// Example 6: Delete a key
	fmt.Println("\nDeleting a key...")
	err = c.KV.Delete(ctx, "default", "test", "cache", "user:456")
	if err != nil {
		log.Fatalf("Failed to delete key: %v", err)
	}
	fmt.Println("Key deleted")

	// Verify deletion
	exists, err = c.KV.Exists(ctx, "default", "test", "cache", "user:456")
	if err != nil {
		log.Fatalf("Failed to check existence: %v", err)
	}
	fmt.Printf("Key 'user:456' exists after deletion: %v\n", exists)

	fmt.Println("\nKV store examples completed!")
}
