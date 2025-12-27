package client

import (
	"context"
	"io"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"google.golang.org/grpc/codes"
)

// StreamClient provides operations for working with streams
type StreamClient struct {
	client *Client
}

// Write writes events to a stream and returns assigned offsets
func (s *StreamClient) Write(ctx context.Context, tenant, namespace, stream string, events []Event) ([]int64, error) {
	req := &flowmeshpb.WriteEventsRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "stream", stream),
		Events:       make([]*flowmeshpb.Event, len(events)),
	}

	for i, event := range events {
		req.Events[i] = event.ToProto()
	}

	resp, err := s.client.streamClient.WriteEvents(s.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "write events")
	}

	return resp.Offsets, nil
}

// Read reads messages from a stream starting at a specific offset
func (s *StreamClient) Read(ctx context.Context, tenant, namespace, stream string, partition int32, offset int64, maxMessages int32) ([]*Message, error) {
	req := &flowmeshpb.ReadStreamRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "stream", stream),
		Partition:    partition,
		Offset:       offset,
		MaxMessages:  maxMessages,
	}

	resp, err := s.client.streamClient.ReadStream(s.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "read stream")
	}

	messages := make([]*Message, len(resp.Messages))
	for i, msg := range resp.Messages {
		messages[i] = MessageFromProto(msg)
	}

	return messages, nil
}

// Subscribe subscribes to a stream with consumer group support (server-side streaming)
// Returns a channel of messages and a channel of errors
func (s *StreamClient) Subscribe(ctx context.Context, tenant, namespace, stream, consumerGroup string, opts ...SubscribeOption) (<-chan *Message, <-chan error) {
	options := &SubscribeOptions{
		Partition:   0,
		StartOffset: -1, // Use committed offset
	}
	for _, opt := range opts {
		opt(options)
	}

	req := &flowmeshpb.SubscribeRequest{
		ResourcePath:  buildResourcePath(tenant, namespace, "stream", stream),
		ConsumerGroup: consumerGroup,
		Partition:     options.Partition,
		StartOffset:   options.StartOffset,
	}

	msgChan := make(chan *Message, 100)
	errChan := make(chan error, 1)

	go func() {
		defer close(msgChan)
		defer close(errChan)

		streamClient, err := s.client.streamClient.Subscribe(s.client.withAuth(ctx), req)
		if err != nil {
			errChan <- wrapError(err, "subscribe")
			return
		}

		for {
			msg, err := streamClient.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errChan <- wrapError(err, "receive message")
				return
			}

			// Check if this is an error status
			if msg.Status != nil && msg.Status.Code != 0 {
				errChan <- &Error{
					Code:    codes.Code(msg.Status.Code),
					Message: msg.Status.Message,
					Details: []interface{}{msg.Status.Details},
				}
				return
			}

			if msg.Message != nil {
				msgChan <- MessageFromProto(msg.Message)
			}
		}
	}()

	return msgChan, errChan
}

// CommitOffset commits an offset for a consumer group
func (s *StreamClient) CommitOffset(ctx context.Context, tenant, namespace, stream, consumerGroup string, partition int32, offset int64) error {
	req := &flowmeshpb.CommitOffsetRequest{
		ResourcePath:  buildResourcePath(tenant, namespace, "stream", stream),
		ConsumerGroup: consumerGroup,
		Partition:     partition,
		Offset:        offset,
	}

	_, err := s.client.streamClient.CommitOffset(s.client.withAuth(ctx), req)
	return wrapError(err, "commit offset")
}

// GetOffset retrieves the committed offset for a consumer group
func (s *StreamClient) GetOffset(ctx context.Context, tenant, namespace, stream, consumerGroup string, partition int32) (int64, error) {
	req := &flowmeshpb.GetOffsetRequest{
		ResourcePath:  buildResourcePath(tenant, namespace, "stream", stream),
		ConsumerGroup: consumerGroup,
		Partition:     partition,
	}

	resp, err := s.client.streamClient.GetOffset(s.client.withAuth(ctx), req)
	if err != nil {
		return 0, wrapError(err, "get offset")
	}

	return resp.Offset, nil
}

// GetLatestOffset retrieves the latest offset for a stream partition
func (s *StreamClient) GetLatestOffset(ctx context.Context, tenant, namespace, stream string, partition int32) (int64, error) {
	req := &flowmeshpb.GetLatestOffsetRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "stream", stream),
		Partition:    partition,
	}

	resp, err := s.client.streamClient.GetLatestOffset(s.client.withAuth(ctx), req)
	if err != nil {
		return 0, wrapError(err, "get latest offset")
	}

	return resp.Offset, nil
}

// GetConsumerGroupState retrieves the complete state of a consumer group including lag
func (s *StreamClient) GetConsumerGroupState(ctx context.Context, tenant, namespace, stream, consumerGroup string, partition int32) (*ConsumerGroupState, error) {
	req := &flowmeshpb.GetConsumerGroupStateRequest{
		ResourcePath:  buildResourcePath(tenant, namespace, "stream", stream),
		ConsumerGroup: consumerGroup,
		Partition:     partition,
	}

	resp, err := s.client.streamClient.GetConsumerGroupState(s.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "get consumer group state")
	}

	return ConsumerGroupStateFromProto(resp.State), nil
}
