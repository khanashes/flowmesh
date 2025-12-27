package client

import (
	"testing"
	"time"
)

func TestEnqueueOptions(t *testing.T) {
	opts := &EnqueueOptions{}
	WithDelay(5 * time.Second)(opts)
	WithHeaders(map[string]string{"key": "value"})(opts)

	if opts.Delay != 5*time.Second {
		t.Errorf("Expected delay 5s, got %v", opts.Delay)
	}
	if opts.Headers["key"] != "value" {
		t.Errorf("Expected header 'value', got '%s'", opts.Headers["key"])
	}
}

func TestReserveOptions(t *testing.T) {
	opts := &ReserveOptions{}
	WithVisibilityTimeout(30 * time.Second)(opts)
	WithLongPollTimeout(5 * time.Second)(opts)

	if opts.VisibilityTimeout != 30*time.Second {
		t.Errorf("Expected visibility timeout 30s, got %v", opts.VisibilityTimeout)
	}
	if opts.LongPollTimeout != 5*time.Second {
		t.Errorf("Expected long poll timeout 5s, got %v", opts.LongPollTimeout)
	}
}

func TestReceiveOptions(t *testing.T) {
	opts := &ReceiveOptions{}
	WithMaxJobs(10)(opts)
	WithReceiveVisibilityTimeout(30 * time.Second)(opts)

	if opts.MaxJobs != 10 {
		t.Errorf("Expected max jobs 10, got %d", opts.MaxJobs)
	}
	if opts.VisibilityTimeout != 30*time.Second {
		t.Errorf("Expected visibility timeout 30s, got %v", opts.VisibilityTimeout)
	}
}

func TestWithMaxJobsBoundaries(t *testing.T) {
	opts := &ReceiveOptions{}

	// Test lower bound
	WithMaxJobs(0)(opts)
	if opts.MaxJobs != 1 {
		t.Errorf("Expected max jobs clamped to 1, got %d", opts.MaxJobs)
	}

	// Test upper bound
	WithMaxJobs(200)(opts)
	if opts.MaxJobs != 100 {
		t.Errorf("Expected max jobs clamped to 100, got %d", opts.MaxJobs)
	}
}

func TestKVSetOptions(t *testing.T) {
	opts := &KVSetOptions{}
	WithTTL(1 * time.Hour)(opts)

	if opts.TTL != 1*time.Hour {
		t.Errorf("Expected TTL 1h, got %v", opts.TTL)
	}
}

func TestSubscribeOptions(t *testing.T) {
	opts := &SubscribeOptions{}
	WithPartition(1)(opts)
	WithStartOffset(-2)(opts)

	if opts.Partition != 1 {
		t.Errorf("Expected partition 1, got %d", opts.Partition)
	}
	if opts.StartOffset != -2 {
		t.Errorf("Expected start offset -2, got %d", opts.StartOffset)
	}
}
