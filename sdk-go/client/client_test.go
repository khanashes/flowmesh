package client

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	// Test that client can be created (will fail connection, but shouldn't panic)
	_, err := NewClient("localhost:99999") // Non-existent server
	if err != nil {
		// Expected to fail connection
		t.Logf("Expected connection error: %v", err)
	}
}

func TestWithAuthToken(t *testing.T) {
	client := &Client{}
	opt := WithAuthToken("test-token")
	opt(client)

	if client.authToken != "test-token" {
		t.Errorf("Expected auth token 'test-token', got '%s'", client.authToken)
	}
}

func TestBuildResourcePath(t *testing.T) {
	path := buildResourcePath("tenant", "namespace", "stream", "name")
	if path.Tenant != "tenant" {
		t.Errorf("Expected tenant 'tenant', got '%s'", path.Tenant)
	}
	if path.Namespace != "namespace" {
		t.Errorf("Expected namespace 'namespace', got '%s'", path.Namespace)
	}
	if path.ResourceType != "stream" {
		t.Errorf("Expected resource type 'stream', got '%s'", path.ResourceType)
	}
	if path.Name != "name" {
		t.Errorf("Expected name 'name', got '%s'", path.Name)
	}
}

func TestErrorWrapping(t *testing.T) {
	err := wrapError(nil, "test")
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}
