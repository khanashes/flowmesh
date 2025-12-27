package client

import (
	"testing"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
)

func TestEventToProto(t *testing.T) {
	event := Event{
		Payload: []byte("test payload"),
		Headers: map[string]string{"key": "value"},
	}

	proto := event.ToProto()
	if string(proto.Payload) != "test payload" {
		t.Errorf("Expected payload 'test payload', got '%s'", string(proto.Payload))
	}
	if proto.Headers["key"] != "value" {
		t.Errorf("Expected header 'value', got '%s'", proto.Headers["key"])
	}
}

func TestMessageFromProto(t *testing.T) {
	now := time.Now().UnixNano()
	proto := &flowmeshpb.Message{
		Id:            "msg-123",
		ResourcePath:  "tenant/namespace/stream/name",
		Partition:     0,
		Offset:        100,
		Payload:       []byte("payload"),
		Headers:       map[string]string{"key": "value"},
		CreatedAt:     now,
		SchemaVersion: 1,
	}

	msg := MessageFromProto(proto)
	if msg.ID != "msg-123" {
		t.Errorf("Expected ID 'msg-123', got '%s'", msg.ID)
	}
	if msg.Offset != 100 {
		t.Errorf("Expected offset 100, got %d", msg.Offset)
	}
	if string(msg.Payload) != "payload" {
		t.Errorf("Expected payload 'payload', got '%s'", string(msg.Payload))
	}
}

func TestJobFromProto(t *testing.T) {
	now := time.Now().UnixNano()
	proto := &flowmeshpb.Job{
		Id:           "job-123",
		ResourcePath: "tenant/namespace/queue/name",
		Seq:          50,
		Payload:      []byte("job payload"),
		Headers:      map[string]string{"priority": "high"},
		CreatedAt:    now,
		VisibleAt:    now,
		ReserveUntil: now,
		Attempts:     1,
	}

	job := JobFromProto(proto)
	if job.ID != "job-123" {
		t.Errorf("Expected ID 'job-123', got '%s'", job.ID)
	}
	if job.Seq != 50 {
		t.Errorf("Expected seq 50, got %d", job.Seq)
	}
	if job.Attempts != 1 {
		t.Errorf("Expected attempts 1, got %d", job.Attempts)
	}
}

func TestRetryPolicyConversion(t *testing.T) {
	policy := &RetryPolicy{
		MaxAttempts:           3,
		InitialBackoffSeconds: 1,
		MaxBackoffSeconds:     60,
		BackoffMultiplier:     2.0,
		BackoffStrategy:       "exponential",
	}

	proto := policy.ToProto()
	if proto.MaxAttempts != 3 {
		t.Errorf("Expected max attempts 3, got %d", proto.MaxAttempts)
	}
	if proto.BackoffStrategy != "exponential" {
		t.Errorf("Expected strategy 'exponential', got '%s'", proto.BackoffStrategy)
	}

	// Convert back
	policy2 := RetryPolicyFromProto(proto)
	if policy2.MaxAttempts != 3 {
		t.Errorf("Expected max attempts 3 after conversion, got %d", policy2.MaxAttempts)
	}
}
