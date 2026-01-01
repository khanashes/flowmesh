package grpc

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/storage"
	replayerrors "github.com/flowmesh/engine/internal/storage/replay"
)

// ReplayService implements the ReplayService gRPC service
type ReplayService struct {
	flowmeshpb.UnimplementedReplayServiceServer
	storage storage.StorageBackend
}

// NewReplayService creates a new replay service
func NewReplayService(storage storage.StorageBackend) *ReplayService {
	return &ReplayService{
		storage: storage,
	}
}

// CreateReplaySession creates a new replay session
func (s *ReplayService) CreateReplaySession(ctx context.Context, req *flowmeshpb.CreateReplaySessionRequest) (*flowmeshpb.CreateReplaySessionResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.SandboxConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_consumer_group is required")
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

	// Determine start offset or time
	var startOffset int64 = -1
	var startTime *time.Time
	if req.StartTimestamp > 0 {
		t := time.Unix(0, req.StartTimestamp)
		startTime = &t
	} else if req.StartOffset >= 0 {
		startOffset = req.StartOffset
	} else {
		return nil, status.Error(codes.InvalidArgument, "either start_offset or start_timestamp must be provided")
	}

	// Determine end offset or time
	var endOffset *int64
	var endTime *time.Time
	if req.EndTimestamp > 0 {
		t := time.Unix(0, req.EndTimestamp)
		endTime = &t
	} else if req.EndOffset >= 0 {
		endOffset = &req.EndOffset
	}

	// Create session
	replayMgr := s.storage.ReplayManager()
	session, err := replayMgr.CreateSession(ctx, resourcePath, startOffset, startTime, endOffset, endTime, req.SandboxConsumerGroup)
	if err != nil {
		return nil, s.mapReplayError(err)
	}

	// Convert to proto
	protoSession := s.sessionToProto(session)

	// Get progress (ignore errors as progress is optional)
	progress, err := replayMgr.GetReplayProgress(ctx, session.ID)
	if err == nil && progress != nil {
		protoSession.Progress = s.progressToProto(progress)
	}

	return &flowmeshpb.CreateReplaySessionResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay session created successfully",
		},
		Session: protoSession,
	}, nil
}

// GetReplaySession retrieves a replay session by ID
func (s *ReplayService) GetReplaySession(ctx context.Context, req *flowmeshpb.GetReplaySessionRequest) (*flowmeshpb.GetReplaySessionResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	replayMgr := s.storage.ReplayManager()
	session, err := replayMgr.GetSession(ctx, req.SessionId)
	if err != nil {
		return nil, s.mapReplayError(err)
	}

	protoSession := s.sessionToProto(session)

	// Get progress (ignore errors as progress is optional)
	progress, err := replayMgr.GetReplayProgress(ctx, session.ID)
	if err == nil && progress != nil {
		protoSession.Progress = s.progressToProto(progress)
	}

	return &flowmeshpb.GetReplaySessionResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay session retrieved successfully",
		},
		Session: protoSession,
	}, nil
}

// ListReplaySessions lists all replay sessions, optionally filtered by stream
func (s *ReplayService) ListReplaySessions(ctx context.Context, req *flowmeshpb.ListReplaySessionsRequest) (*flowmeshpb.ListReplaySessionsResponse, error) {
	var stream string
	if req.ResourcePath != nil {
		resourcePath, err := validation.BuildResourcePath(
			req.ResourcePath.Tenant,
			req.ResourcePath.Namespace,
			req.ResourcePath.ResourceType,
			req.ResourcePath.Name,
		)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		stream = resourcePath
	}

	replayMgr := s.storage.ReplayManager()
	sessions, err := replayMgr.ListSessions(ctx, stream)
	if err != nil {
		return nil, s.mapReplayError(err)
	}

	protoSessions := make([]*flowmeshpb.ReplaySession, len(sessions))
	for i, session := range sessions {
		protoSession := s.sessionToProto(session)
		// Get progress for each session
		progress, _ := replayMgr.GetReplayProgress(ctx, session.ID)
		if progress != nil {
			protoSession.Progress = s.progressToProto(progress)
		}
		protoSessions[i] = protoSession
	}

	return &flowmeshpb.ListReplaySessionsResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay sessions listed successfully",
		},
		Sessions: protoSessions,
	}, nil
}

// StartReplay starts replaying messages for a session
func (s *ReplayService) StartReplay(ctx context.Context, req *flowmeshpb.StartReplayRequest) (*flowmeshpb.StartReplayResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	replayMgr := s.storage.ReplayManager()
	if err := replayMgr.StartReplay(ctx, req.SessionId); err != nil {
		return nil, s.mapReplayError(err)
	}

	return &flowmeshpb.StartReplayResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay started successfully",
		},
	}, nil
}

// PauseReplay pauses an active replay session
func (s *ReplayService) PauseReplay(ctx context.Context, req *flowmeshpb.PauseReplayRequest) (*flowmeshpb.PauseReplayResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	replayMgr := s.storage.ReplayManager()
	if err := replayMgr.PauseReplay(ctx, req.SessionId); err != nil {
		return nil, s.mapReplayError(err)
	}

	return &flowmeshpb.PauseReplayResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay paused successfully",
		},
	}, nil
}

// ResumeReplay resumes a paused replay session
func (s *ReplayService) ResumeReplay(ctx context.Context, req *flowmeshpb.ResumeReplayRequest) (*flowmeshpb.ResumeReplayResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	replayMgr := s.storage.ReplayManager()
	if err := replayMgr.ResumeReplay(ctx, req.SessionId); err != nil {
		return nil, s.mapReplayError(err)
	}

	return &flowmeshpb.ResumeReplayResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay resumed successfully",
		},
	}, nil
}

// StopReplay stops an active replay session
func (s *ReplayService) StopReplay(ctx context.Context, req *flowmeshpb.StopReplayRequest) (*flowmeshpb.StopReplayResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	replayMgr := s.storage.ReplayManager()
	if err := replayMgr.StopReplay(ctx, req.SessionId); err != nil {
		return nil, s.mapReplayError(err)
	}

	return &flowmeshpb.StopReplayResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay stopped successfully",
		},
	}, nil
}

// DeleteReplaySession deletes a replay session
func (s *ReplayService) DeleteReplaySession(ctx context.Context, req *flowmeshpb.DeleteReplaySessionRequest) (*flowmeshpb.DeleteReplaySessionResponse, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	replayMgr := s.storage.ReplayManager()
	if err := replayMgr.DeleteSession(ctx, req.SessionId); err != nil {
		return nil, s.mapReplayError(err)
	}

	return &flowmeshpb.DeleteReplaySessionResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "replay session deleted successfully",
		},
	}, nil
}

// Helper functions

func (s *ReplayService) sessionToProto(session *storage.ReplaySession) *flowmeshpb.ReplaySession {
	protoSession := &flowmeshpb.ReplaySession{
		SessionId:            session.ID,
		Stream:               session.Stream,
		Partition:            session.Partition,
		StartOffset:          session.StartOffset,
		EndOffset:            -1,
		SandboxConsumerGroup: session.SandboxConsumerGroup,
		Status:               session.Status,
		CreatedAt:            session.CreatedAt.UnixNano(),
		UpdatedAt:            session.UpdatedAt.UnixNano(),
	}

	if session.StartTime != nil {
		protoSession.StartTimestamp = session.StartTime.UnixNano()
	} else {
		protoSession.StartTimestamp = 0
	}

	if session.EndOffset != nil {
		protoSession.EndOffset = *session.EndOffset
	}

	if session.EndTime != nil {
		protoSession.EndTimestamp = session.EndTime.UnixNano()
	} else {
		protoSession.EndTimestamp = 0
	}

	return protoSession
}

func (s *ReplayService) progressToProto(progress *storage.ReplayProgress) *flowmeshpb.ReplayProgress {
	protoProgress := &flowmeshpb.ReplayProgress{
		CurrentOffset:    progress.CurrentOffset,
		MessagesReplayed: progress.MessagesReplayed,
		Errors:           progress.Errors,
	}

	if progress.StartedAt != nil {
		protoProgress.StartedAt = progress.StartedAt.UnixNano()
	}
	if progress.PausedAt != nil {
		protoProgress.PausedAt = progress.PausedAt.UnixNano()
	}
	if progress.CompletedAt != nil {
		protoProgress.CompletedAt = progress.CompletedAt.UnixNano()
	}

	return protoProgress
}

func (s *ReplayService) mapReplayError(err error) error {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case replayerrors.SessionNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case replayerrors.InvalidSessionStateError:
		return status.Error(codes.FailedPrecondition, e.Error())
	case replayerrors.StreamNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case replayerrors.InvalidOffsetError:
		return status.Error(codes.InvalidArgument, e.Error())
	case replayerrors.InvalidTimestampError:
		return status.Error(codes.InvalidArgument, e.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
