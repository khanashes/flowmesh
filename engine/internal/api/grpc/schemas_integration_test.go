package grpc

import (
	"context"
	"testing"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSchemaIntegrationTest(t *testing.T) (storage.StorageBackend, func()) {
	tmpDir := t.TempDir()

	storageBackend, err := storage.NewBuilder().
		WithDataDir(tmpDir).
		Build()
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		storageBackend.Stop(ctx)
	}

	return storageBackend, cleanup
}

func TestSchemaService_Integration_RegisterAndRetrieve(t *testing.T) {
	storageBackend, cleanup := setupSchemaIntegrationTest(t)
	defer cleanup()

	service := NewSchemaService(storageBackend)
	ctx := context.Background()

	// Register schema
	registerReq := &flowmeshpb.RegisterSchemaRequest{
		Tenant:     "test-tenant",
		SchemaId:   "user-schema",
		SchemaType: "jsonschema",
		Version:    1,
		Definition: []byte(`{
			"type": "object",
			"properties": {
				"name": {"type": "string"},
				"age": {"type": "number"}
			},
			"required": ["name"]
		}`),
	}

	registerResp, err := service.RegisterSchema(ctx, registerReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), registerResp.Status.Code) // codes.OK = 0

	// Retrieve schema
	getReq := &flowmeshpb.GetSchemaRequest{
		Tenant:   "test-tenant",
		SchemaId: "user-schema",
		Version:  1,
	}

	getResp, err := service.GetSchema(ctx, getReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), getResp.Status.Code)
	assert.NotNil(t, getResp.Schema)
	assert.Equal(t, "user-schema", getResp.Schema.Id)
	assert.Equal(t, int32(1), getResp.Schema.Version)
}

func TestSchemaService_Integration_StreamValidation(t *testing.T) {
	storageBackend, cleanup := setupSchemaIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	metaStore := storageBackend.MetaStore()
	streamMgr := storageBackend.StreamManager()
	service := NewSchemaService(storageBackend)

	tenant := "test-tenant"
	namespace := "test-ns"
	streamName := "test-stream"
	resourcePath := tenant + "/" + namespace + "/stream/" + streamName

	// Create stream resource
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       streamName,
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Register schema
	schemaID := "event-schema"
	registerReq := &flowmeshpb.RegisterSchemaRequest{
		Tenant:     tenant,
		SchemaId:   schemaID,
		SchemaType: "jsonschema",
		Version:    1,
		Definition: []byte(`{
			"type": "object",
			"properties": {
				"event_type": {"type": "string"},
				"data": {"type": "object"}
			},
			"required": ["event_type"]
		}`),
	}
	_, err = service.RegisterSchema(ctx, registerReq)
	require.NoError(t, err)

	// Attach schema to stream
	setSchemaReq := &flowmeshpb.SetResourceSchemaRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       tenant,
			Namespace:    namespace,
			ResourceType: "stream",
			Name:         streamName,
		},
		SchemaId: schemaID,
		Version:  1,
	}
	_, err = service.SetResourceSchema(ctx, setSchemaReq)
	require.NoError(t, err)

	// Initialize stream
	err = streamMgr.InitializeStream(ctx, resourcePath)
	require.NoError(t, err)

	// Write valid event
	validEvent := storage.StreamEvent{
		Payload: []byte(`{"event_type": "user_created", "data": {"name": "John"}}`),
		Headers: map[string]string{},
	}
	offsets, err := streamMgr.WriteEvents(ctx, resourcePath, []storage.StreamEvent{validEvent})
	require.NoError(t, err)
	assert.Len(t, offsets, 1)

	// Write invalid event (missing required field)
	invalidEvent := storage.StreamEvent{
		Payload: []byte(`{"data": {"name": "John"}}`), // missing event_type
		Headers: map[string]string{},
	}
	_, err = streamMgr.WriteEvents(ctx, resourcePath, []storage.StreamEvent{invalidEvent})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}

func TestSchemaService_Integration_QueueValidation(t *testing.T) {
	storageBackend, cleanup := setupSchemaIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	metaStore := storageBackend.MetaStore()
	queueMgr := storageBackend.QueueManager()
	service := NewSchemaService(storageBackend)

	tenant := "test-tenant"
	namespace := "test-ns"
	queueName := "test-queue"
	resourcePath := tenant + "/" + namespace + "/queue/" + queueName

	// Create queue resource
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       queueName,
		Type:       metastore.ResourceQueue,
		Partitions: 1,
	}
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Register schema
	schemaID := "job-schema"
	registerReq := &flowmeshpb.RegisterSchemaRequest{
		Tenant:     tenant,
		SchemaId:   schemaID,
		SchemaType: "jsonschema",
		Version:    1,
		Definition: []byte(`{
			"type": "object",
			"properties": {
				"task": {"type": "string"},
				"params": {"type": "object"}
			},
			"required": ["task"]
		}`),
	}
	_, err = service.RegisterSchema(ctx, registerReq)
	require.NoError(t, err)

	// Attach schema to queue
	setSchemaReq := &flowmeshpb.SetResourceSchemaRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       tenant,
			Namespace:    namespace,
			ResourceType: "queue",
			Name:         queueName,
		},
		SchemaId: schemaID,
		Version:  1,
	}
	_, err = service.SetResourceSchema(ctx, setSchemaReq)
	require.NoError(t, err)

	// Initialize queue
	err = queueMgr.InitializeQueue(ctx, resourcePath)
	require.NoError(t, err)

	// Enqueue valid job
	validPayload := []byte(`{"task": "process_order", "params": {"order_id": 123}}`)
	jobID, seq, err := queueMgr.Enqueue(ctx, resourcePath, validPayload, storage.QueueEnqueueOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, jobID)
	assert.GreaterOrEqual(t, seq, int64(0))

	// Enqueue invalid job (missing required field)
	invalidPayload := []byte(`{"params": {"order_id": 123}}`) // missing task
	_, _, err = queueMgr.Enqueue(ctx, resourcePath, invalidPayload, storage.QueueEnqueueOptions{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}

func TestSchemaService_Integration_NoSchema(t *testing.T) {
	storageBackend, cleanup := setupSchemaIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	metaStore := storageBackend.MetaStore()
	streamMgr := storageBackend.StreamManager()

	tenant := "test-tenant"
	namespace := "test-ns"
	streamName := "test-stream"
	resourcePath := tenant + "/" + namespace + "/stream/" + streamName

	// Create stream resource without schema
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       streamName,
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Initialize stream
	err = streamMgr.InitializeStream(ctx, resourcePath)
	require.NoError(t, err)

	// Write event without schema (should succeed)
	event := storage.StreamEvent{
		Payload: []byte(`{"any": "data"}`),
		Headers: map[string]string{},
	}
	offsets, err := streamMgr.WriteEvents(ctx, resourcePath, []storage.StreamEvent{event})
	require.NoError(t, err)
	assert.Len(t, offsets, 1)
}

func TestSchemaService_Integration_SchemaVersioning(t *testing.T) {
	storageBackend, cleanup := setupSchemaIntegrationTest(t)
	defer cleanup()

	ctx := context.Background()
	metaStore := storageBackend.MetaStore()
	streamMgr := storageBackend.StreamManager()
	service := NewSchemaService(storageBackend)

	tenant := "test-tenant"
	namespace := "test-ns"
	streamName := "test-stream"
	resourcePath := tenant + "/" + namespace + "/stream/" + streamName

	// Create stream resource
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  namespace,
		Name:       streamName,
		Type:       metastore.ResourceStream,
		Partitions: 1,
	}
	err := metaStore.CreateResource(config)
	require.NoError(t, err)

	// Register schema version 1
	schemaID := "test-schema"
	registerReqV1 := &flowmeshpb.RegisterSchemaRequest{
		Tenant:     tenant,
		SchemaId:   schemaID,
		SchemaType: "jsonschema",
		Version:    1,
		Definition: []byte(`{
			"type": "object",
			"properties": {
				"name": {"type": "string"}
			}
		}`),
	}
	_, err = service.RegisterSchema(ctx, registerReqV1)
	require.NoError(t, err)

	// Register schema version 2
	registerReqV2 := &flowmeshpb.RegisterSchemaRequest{
		Tenant:     tenant,
		SchemaId:   schemaID,
		SchemaType: "jsonschema",
		Version:    2,
		Definition: []byte(`{
			"type": "object",
			"properties": {
				"name": {"type": "string"},
				"email": {"type": "string"}
			}
		}`),
	}
	_, err = service.RegisterSchema(ctx, registerReqV2)
	require.NoError(t, err)

	// Attach schema version 1 to stream
	setSchemaReq := &flowmeshpb.SetResourceSchemaRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       tenant,
			Namespace:    namespace,
			ResourceType: "stream",
			Name:         streamName,
		},
		SchemaId: schemaID,
		Version:  1,
	}
	_, err = service.SetResourceSchema(ctx, setSchemaReq)
	require.NoError(t, err)

	// Initialize stream
	err = streamMgr.InitializeStream(ctx, resourcePath)
	require.NoError(t, err)

	// Write event valid for version 1
	validEvent := storage.StreamEvent{
		Payload: []byte(`{"name": "John"}`),
		Headers: map[string]string{},
	}
	offsets, err := streamMgr.WriteEvents(ctx, resourcePath, []storage.StreamEvent{validEvent})
	require.NoError(t, err)
	assert.Len(t, offsets, 1)

	// Update to version 2
	setSchemaReqV2 := &flowmeshpb.SetResourceSchemaRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       tenant,
			Namespace:    namespace,
			ResourceType: "stream",
			Name:         streamName,
		},
		SchemaId: schemaID,
		Version:  2,
	}
	_, err = service.SetResourceSchema(ctx, setSchemaReqV2)
	require.NoError(t, err)

	// Write event valid for version 2
	validEventV2 := storage.StreamEvent{
		Payload: []byte(`{"name": "John", "email": "john@example.com"}`),
		Headers: map[string]string{},
	}
	offsets, err = streamMgr.WriteEvents(ctx, resourcePath, []storage.StreamEvent{validEventV2})
	require.NoError(t, err)
	assert.Len(t, offsets, 1)
}
