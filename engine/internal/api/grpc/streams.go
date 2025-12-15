package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/consumers"
	streamserrors "github.com/flowmesh/engine/internal/storage/streams"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StreamService implements the StreamService gRPC service
type StreamService struct {
	flowmeshpb.UnimplementedStreamServiceServer
	storage storage.StorageBackend
	log     zerolog.Logger
}

// NewStreamService creates a new stream service
func NewStreamService(storage storage.StorageBackend) *StreamService {
	return &StreamService{
		storage: storage,
		log:     logger.WithComponent("grpc.streams"),
	}
}

// WriteEvents writes multiple events to a stream and returns assigned offsets
func (s *StreamService) WriteEvents(ctx context.Context, req *flowmeshpb.WriteEventsRequest) (*flowmeshpb.WriteEventsResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if len(req.Events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one event is required")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Convert proto events to storage events
	events := make([]storage.StreamEvent, 0, len(req.Events))
	for i, event := range req.Events {
		if len(event.Payload) == 0 {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("event %d: payload cannot be empty", i))
		}
		events = append(events, storage.StreamEvent{
			Payload: event.Payload,
			Headers: event.Headers,
		})
	}

	// Write events
	streamMgr := s.storage.StreamManager()
	offsets, err := streamMgr.WriteEvents(ctx, resourcePath, events)
	if err != nil {
		return nil, s.mapStreamError(err, resourcePath)
	}

	return &flowmeshpb.WriteEventsResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "events written successfully",
		},
		Offsets: offsets,
	}, nil
}

// ReadStream reads messages from a stream starting at a specific offset
func (s *StreamService) ReadStream(ctx context.Context, req *flowmeshpb.ReadStreamRequest) (*flowmeshpb.ReadStreamResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.Offset < 0 {
		return nil, status.Error(codes.InvalidArgument, "offset cannot be negative")
	}
	if req.MaxMessages <= 0 {
		req.MaxMessages = 100 // Default to 100 messages
	}
	if req.MaxMessages > 1000 {
		req.MaxMessages = 1000 // Cap at 1000 messages
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Default partition to 0
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// Read messages
	streamMgr := s.storage.StreamManager()
	messages, err := streamMgr.ReadFromOffset(ctx, resourcePath, partition, req.Offset, int(req.MaxMessages))
	if err != nil {
		return nil, s.mapStreamError(err, resourcePath)
	}

	// Convert storage messages to proto messages
	protoMessages := make([]*flowmeshpb.Message, 0, len(messages))
	for _, msg := range messages {
		protoMessages = append(protoMessages, &flowmeshpb.Message{
			Id:            msg.ID,
			ResourcePath:  msg.ResourcePath,
			Partition:     msg.Partition,
			Offset:        msg.Offset,
			Payload:       msg.Payload,
			Headers:       msg.Headers,
			CreatedAt:     msg.CreatedAt.UnixNano(),
			SchemaVersion: msg.SchemaVersion,
		})
	}

	return &flowmeshpb.ReadStreamResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "messages read successfully",
		},
		Messages: protoMessages,
	}, nil
}

// Subscribe subscribes to a stream with consumer group support (server-side streaming)
func (s *StreamService) Subscribe(req *flowmeshpb.SubscribeRequest, stream flowmeshpb.StreamService_SubscribeServer) error {
	ctx := stream.Context()

	// Validate request
	if req.ResourcePath == nil {
		return status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.ConsumerGroup == "" {
		return status.Error(codes.InvalidArgument, "consumer_group is required")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Default partition to 0
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// Determine start offset
	startOffset := req.StartOffset
	if startOffset == -1 {
		// Use committed offset
		consumerMgr := s.storage.ConsumerGroupManager()
		committedOffset, err := consumerMgr.GetCommittedOffset(ctx, resourcePath, req.ConsumerGroup, partition)
		if err != nil {
			return s.mapConsumerError(err, resourcePath, req.ConsumerGroup)
		}
		if committedOffset < 0 {
			startOffset = 0 // Start from beginning if no commits
		} else {
			startOffset = committedOffset + 1 // Start from next offset after committed
		}
	} else if startOffset == -2 {
		// Use latest offset
		streamMgr := s.storage.StreamManager()
		latestOffset, err := streamMgr.GetLatestOffset(ctx, resourcePath, partition)
		if err != nil {
			return s.mapStreamError(err, resourcePath)
		}
		if latestOffset < 0 {
			startOffset = 0
		} else {
			startOffset = latestOffset + 1 // Will wait for new messages
		}
	} else if startOffset < 0 {
		return status.Error(codes.InvalidArgument, "start_offset must be >= 0, -1 (committed), or -2 (latest)")
	}

	streamMgr := s.storage.StreamManager()

	// Polling loop
	ticker := time.NewTicker(100 * time.Millisecond) // Poll every 100ms
	defer ticker.Stop()

	currentOffset := startOffset
	maxMessagesPerPoll := 10

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Read messages from current offset
			messages, err := streamMgr.ReadFromOffset(ctx, resourcePath, partition, currentOffset, maxMessagesPerPoll)
			if err != nil {
				// Send error and continue
				if sendErr := stream.Send(&flowmeshpb.SubscribeResponse{
					Status: &flowmeshpb.Status{
						Code:    int32(codes.Internal),
						Message: err.Error(),
					},
				}); sendErr != nil {
					return sendErr
				}
				continue
			}

			// Send messages
			for _, msg := range messages {
				protoMsg := &flowmeshpb.Message{
					Id:            msg.ID,
					ResourcePath:  msg.ResourcePath,
					Partition:     msg.Partition,
					Offset:        msg.Offset,
					Payload:       msg.Payload,
					Headers:       msg.Headers,
					CreatedAt:     msg.CreatedAt.UnixNano(),
					SchemaVersion: msg.SchemaVersion,
				}

				if err := stream.Send(&flowmeshpb.SubscribeResponse{
					Message: protoMsg,
				}); err != nil {
					return err
				}

				// Update current offset
				currentOffset = msg.Offset + 1
			}
		}
	}
}

// CommitOffset commits an offset for a consumer group
func (s *StreamService) CommitOffset(ctx context.Context, req *flowmeshpb.CommitOffsetRequest) (*flowmeshpb.CommitOffsetResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "consumer_group is required")
	}
	if req.Offset < -1 {
		return nil, status.Error(codes.InvalidArgument, "offset cannot be less than -1")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Default partition to 0
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// Commit offset
	consumerMgr := s.storage.ConsumerGroupManager()
	err = consumerMgr.CommitOffset(ctx, resourcePath, req.ConsumerGroup, partition, req.Offset)
	if err != nil {
		return nil, s.mapConsumerError(err, resourcePath, req.ConsumerGroup)
	}

	return &flowmeshpb.CommitOffsetResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "offset committed successfully",
		},
	}, nil
}

// GetOffset retrieves the committed offset for a consumer group
func (s *StreamService) GetOffset(ctx context.Context, req *flowmeshpb.GetOffsetRequest) (*flowmeshpb.GetOffsetResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "consumer_group is required")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Default partition to 0
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// Get committed offset
	consumerMgr := s.storage.ConsumerGroupManager()
	offset, err := consumerMgr.GetCommittedOffset(ctx, resourcePath, req.ConsumerGroup, partition)
	if err != nil {
		return nil, s.mapConsumerError(err, resourcePath, req.ConsumerGroup)
	}

	return &flowmeshpb.GetOffsetResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "offset retrieved successfully",
		},
		Offset: offset,
	}, nil
}

// GetLatestOffset retrieves the latest offset for a stream partition
func (s *StreamService) GetLatestOffset(ctx context.Context, req *flowmeshpb.GetLatestOffsetRequest) (*flowmeshpb.GetLatestOffsetResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Default partition to 0
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// Get latest offset
	streamMgr := s.storage.StreamManager()
	offset, err := streamMgr.GetLatestOffset(ctx, resourcePath, partition)
	if err != nil {
		return nil, s.mapStreamError(err, resourcePath)
	}

	return &flowmeshpb.GetLatestOffsetResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "latest offset retrieved successfully",
		},
		Offset: offset,
	}, nil
}

// GetConsumerGroupState retrieves the complete state of a consumer group including lag
func (s *StreamService) GetConsumerGroupState(ctx context.Context, req *flowmeshpb.GetConsumerGroupStateRequest) (*flowmeshpb.GetConsumerGroupStateResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "consumer_group is required")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is stream
	if req.ResourcePath.ResourceType != "stream" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream'")
	}

	// Default partition to 0
	partition := req.Partition
	if partition < 0 {
		partition = 0
	}

	// Get consumer group state
	consumerMgr := s.storage.ConsumerGroupManager()
	state, err := consumerMgr.GetConsumerGroupState(ctx, resourcePath, req.ConsumerGroup, partition)
	if err != nil {
		return nil, s.mapConsumerError(err, resourcePath, req.ConsumerGroup)
	}

	return &flowmeshpb.GetConsumerGroupStateResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "consumer group state retrieved successfully",
		},
		State: &flowmeshpb.ConsumerGroupState{
			Stream:          state.Stream,
			Group:           state.Group,
			Partition:       state.Partition,
			CommittedOffset: state.CommittedOffset,
			LatestOffset:    state.LatestOffset,
			Lag:             state.Lag,
		},
	}, nil
}

// mapStreamError maps storage stream errors to gRPC status codes
func (s *StreamService) mapStreamError(err error, resourcePath string) error {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case streamserrors.StreamNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case streamserrors.InvalidOffsetError:
		return status.Error(codes.InvalidArgument, e.Error())
	case streamserrors.ReadError:
		return status.Error(codes.Internal, e.Error())
	case streamserrors.WriteError:
		return status.Error(codes.Internal, e.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// mapConsumerError maps consumer group errors to gRPC status codes
func (s *StreamService) mapConsumerError(err error, resourcePath, group string) error {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case consumers.ConsumerGroupNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case consumers.InvalidOffsetError:
		return status.Error(codes.InvalidArgument, e.Error())
	case consumers.CommitOffsetError:
		return status.Error(codes.Internal, e.Error())
	case consumers.GetOffsetError:
		return status.Error(codes.Internal, e.Error())
	case consumers.CalculateLagError:
		return status.Error(codes.Internal, e.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
