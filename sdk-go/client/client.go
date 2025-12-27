package client

import (
	"context"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client is the main FlowMesh client
type Client struct {
	conn      *grpc.ClientConn
	authToken string

	// Service clients
	healthClient flowmeshpb.HealthServiceClient
	streamClient flowmeshpb.StreamServiceClient
	queueClient  flowmeshpb.QueueServiceClient
	kvClient     flowmeshpb.KVServiceClient

	// Sub-clients
	Stream *StreamClient
	Queue  *QueueClient
	KV     *KVClient
}

// NewClient creates a new FlowMesh client
func NewClient(addr string, opts ...Option) (*Client, error) {
	// Establish gRPC connection
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn: conn,
	}

	// Apply options
	for _, opt := range opts {
		opt(c)
	}

	// Initialize service clients
	c.healthClient = flowmeshpb.NewHealthServiceClient(conn)
	c.streamClient = flowmeshpb.NewStreamServiceClient(conn)
	c.queueClient = flowmeshpb.NewQueueServiceClient(conn)
	c.kvClient = flowmeshpb.NewKVServiceClient(conn)

	// Initialize sub-clients
	c.Stream = &StreamClient{client: c}
	c.Queue = &QueueClient{client: c}
	c.KV = &KVClient{client: c}

	return c, nil
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// withAuth adds authentication metadata to context if token is set
func (c *Client) withAuth(ctx context.Context) context.Context {
	if c.authToken != "" {
		md := metadata.New(map[string]string{
			"authorization": "Bearer " + c.authToken,
		})
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// HealthCheck checks if the server is running
func (c *Client) HealthCheck(ctx context.Context) error {
	_, err := c.healthClient.HealthCheck(c.withAuth(ctx), &flowmeshpb.HealthCheckRequest{})
	return wrapError(err, "health check")
}

// ReadinessCheck checks if the server is ready to serve requests
func (c *Client) ReadinessCheck(ctx context.Context) (bool, error) {
	resp, err := c.healthClient.ReadinessCheck(c.withAuth(ctx), &flowmeshpb.ReadinessCheckRequest{})
	if err != nil {
		return false, wrapError(err, "readiness check")
	}
	return resp.Ready, nil
}
