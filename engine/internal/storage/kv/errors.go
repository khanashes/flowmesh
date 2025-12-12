package kv

import "fmt"

// KVStoreNotFoundError indicates a KV store resource was not found
type KVStoreNotFoundError struct {
	ResourcePath string
}

func (e KVStoreNotFoundError) Error() string {
	return fmt.Sprintf("KV store not found: %s", e.ResourcePath)
}

// KeyNotFoundError indicates a key was not found
type KeyNotFoundError struct {
	ResourcePath string
	Key          string
}

func (e KeyNotFoundError) Error() string {
	return fmt.Sprintf("key %s not found in KV store %s", e.Key, e.ResourcePath)
}

// KeyExpiredError indicates a key has expired
type KeyExpiredError struct {
	ResourcePath string
	Key          string
}

func (e KeyExpiredError) Error() string {
	return fmt.Sprintf("key %s in KV store %s has expired", e.Key, e.ResourcePath)
}

// InvalidKeyError indicates an invalid key was provided
type InvalidKeyError struct {
	Key    string
	Reason string
}

func (e InvalidKeyError) Error() string {
	return fmt.Sprintf("invalid key %s: %s", e.Key, e.Reason)
}

// InvalidValueError indicates an invalid value was provided
type InvalidValueError struct {
	Reason string
}

func (e InvalidValueError) Error() string {
	return fmt.Sprintf("invalid value: %s", e.Reason)
}
