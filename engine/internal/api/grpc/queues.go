package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// QueueService implements the QueueService gRPC service
type QueueService struct {
	flowmeshpb.UnimplementedQueueServiceServer
	storage storage.StorageBackend
	log     zerolog.Logger
}

// NewQueueService creates a new queue service
func NewQueueService(storage storage.StorageBackend) *QueueService {
	return &QueueService{
		storage: storage,
		log:     logger.WithComponent("grpc.queues"),
	}
}

// Enqueue enqueues a job to a queue and returns job ID and sequence number
func (s *QueueService) Enqueue(
	ctx context.Context,
	req *flowmeshpb.EnqueueRequest,
) (*flowmeshpb.EnqueueResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if len(req.Payload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "payload cannot be empty")
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

	// Validate resource type is queue
	if req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'queue'")
	}

	// Convert delay
	delay := time.Duration(req.DelaySeconds) * time.Second
	if delay < 0 {
		return nil, status.Error(codes.InvalidArgument, "delay_seconds cannot be negative")
	}

	// Enqueue job
	queueMgr := s.storage.QueueManager()
	jobID, seq, err := queueMgr.Enqueue(ctx, resourcePath, req.Payload, storage.QueueEnqueueOptions{
		Delay:   delay,
		Headers: req.Headers,
	})
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	return &flowmeshpb.EnqueueResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "job enqueued successfully",
		},
		JobId: jobID,
		Seq:   seq,
	}, nil
}

// Reserve reserves a single job from the queue with visibility timeout
func (s *QueueService) Reserve(ctx context.Context, req *flowmeshpb.ReserveRequest) (*flowmeshpb.ReserveResponse, error) {
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

	// Validate resource type is queue
	if req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'queue'")
	}

	// Convert visibility timeout (default 30 seconds)
	visibilityTimeout := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}
	if visibilityTimeout > 12*time.Hour {
		return nil, status.Error(codes.InvalidArgument, "visibility_timeout_seconds cannot exceed 43200 (12 hours)")
	}

	// Reserve job
	queueMgr := s.storage.QueueManager()
	job, err := queueMgr.Reserve(ctx, resourcePath, visibilityTimeout)
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	// If no job available, return empty response
	if job == nil {
		return &flowmeshpb.ReserveResponse{
			Status: &flowmeshpb.Status{
				Code:    int32(codes.OK),
				Message: "no jobs available",
			},
		}, nil
	}

	// Get job payload and headers
	payload, headers, err := s.getJobPayloadAndHeaders(ctx, queueMgr, resourcePath, job.ID)
	if err != nil {
		s.log.Warn().Err(err).Str("job_id", job.ID).Msg("Failed to get job payload, returning job without payload")
		payload = []byte{} // Return empty payload if we can't read it
		headers = make(map[string]string)
	}

	// Convert to proto job
	protoJob := &flowmeshpb.Job{
		Id:           job.ID,
		ResourcePath: resourcePath,
		Seq:          job.Seq,
		Payload:      payload,
		Headers:      headers,
		CreatedAt:    job.CreatedAt.UnixNano(),
		VisibleAt:    job.VisibleAt.UnixNano(),
		Attempts:     job.Attempts,
	}

	if !job.ReserveUntil.IsZero() {
		protoJob.ReserveUntil = job.ReserveUntil.UnixNano()
	}

	return &flowmeshpb.ReserveResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "job reserved successfully",
		},
		Job: protoJob,
	}, nil
}

// Receive receives multiple jobs from the queue (batch receive)
func (s *QueueService) Receive(ctx context.Context, req *flowmeshpb.ReceiveRequest) (*flowmeshpb.ReceiveResponse, error) {
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

	// Validate resource type is queue
	if req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'queue'")
	}

	// Validate max jobs
	maxJobs := int(req.MaxJobs)
	if maxJobs <= 0 {
		maxJobs = 1
	}
	if maxJobs > 100 {
		maxJobs = 100
	}

	// Convert visibility timeout (default 30 seconds)
	visibilityTimeout := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}
	if visibilityTimeout > 12*time.Hour {
		return nil, status.Error(codes.InvalidArgument, "visibility_timeout_seconds cannot exceed 43200 (12 hours)")
	}

	// Convert long poll timeout
	longPollTimeout := time.Duration(req.LongPollTimeoutSeconds) * time.Second

	// Receive jobs using batch receive
	queueMgr := s.storage.QueueManager()
	queueJobs, err := queueMgr.Receive(ctx, resourcePath, maxJobs, storage.QueueReserveOptions{
		VisibilityTimeout: visibilityTimeout,
		LongPollTimeout:   longPollTimeout,
		MaxWaitTime:       20 * time.Second, // Default max wait
	})
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	// Convert to proto jobs
	jobs := make([]*flowmeshpb.Job, 0, len(queueJobs))
	for _, job := range queueJobs {
		// Get job payload and headers
		payload, headers, err := s.getJobPayloadAndHeaders(ctx, queueMgr, resourcePath, job.ID)
		if err != nil {
			s.log.Warn().Err(err).Str("job_id", job.ID).Msg("Failed to get job payload")
			payload = []byte{}
			headers = make(map[string]string)
		}

		protoJob := &flowmeshpb.Job{
			Id:           job.ID,
			ResourcePath: resourcePath,
			Seq:          job.Seq,
			Payload:      payload,
			Headers:      headers,
			CreatedAt:    job.CreatedAt.UnixNano(),
			VisibleAt:    job.VisibleAt.UnixNano(),
			Attempts:     job.Attempts,
		}

		if !job.ReserveUntil.IsZero() {
			protoJob.ReserveUntil = job.ReserveUntil.UnixNano()
		}

		jobs = append(jobs, protoJob)
	}

	return &flowmeshpb.ReceiveResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: fmt.Sprintf("received %d jobs", len(jobs)),
		},
		Jobs: jobs,
	}, nil
}

// ACK acknowledges completion of a job
func (s *QueueService) ACK(ctx context.Context, req *flowmeshpb.ACKRequest) (*flowmeshpb.ACKResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
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

	// Validate resource type is queue
	if req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'queue'")
	}

	// ACK job
	queueMgr := s.storage.QueueManager()
	err = queueMgr.RemoveFromInFlight(ctx, resourcePath, req.JobId)
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	return &flowmeshpb.ACKResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "job acknowledged successfully",
		},
	}, nil
}

// NACK negatively acknowledges a job (requeue with backoff)
func (s *QueueService) NACK(ctx context.Context, req *flowmeshpb.NACKRequest) (*flowmeshpb.NACKResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
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

	// Validate resource type is queue
	if req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'queue'")
	}

	// Convert delay (0 means use backoff)
	var delay time.Duration
	if req.DelaySeconds > 0 {
		delay = time.Duration(req.DelaySeconds) * time.Second
	}

	// NACK job
	queueMgr := s.storage.QueueManager()
	if delay > 0 {
		err = queueMgr.NACKWithDelay(ctx, resourcePath, req.JobId, delay)
	} else {
		err = queueMgr.NACK(ctx, resourcePath, req.JobId)
	}
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	return &flowmeshpb.NACKResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "job NACKed successfully",
		},
	}, nil
}

// GetQueueStats retrieves queue statistics
func (s *QueueService) GetQueueStats(
	ctx context.Context,
	req *flowmeshpb.GetQueueStatsRequest,
) (*flowmeshpb.GetQueueStatsResponse, error) {
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

	// Validate resource type is queue
	if req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'queue'")
	}

	// Get queue stats
	queueMgr := s.storage.QueueManager()
	stats, err := queueMgr.GetQueueStats(ctx, resourcePath)
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	return &flowmeshpb.GetQueueStatsResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "queue statistics retrieved successfully",
		},
		Stats: &flowmeshpb.QueueStats{
			TotalJobs:           stats.TotalJobs,
			PendingJobs:         stats.PendingJobs,
			InFlightJobs:        stats.InFlightJobs,
			CompletedJobs:       stats.CompletedJobs,
			FailedJobs:          stats.FailedJobs,
			OldestJobAgeSeconds: int64(stats.OldestJobAge.Seconds()),
		},
	}, nil
}

// Helper methods

// getJobPayloadAndHeaders retrieves both payload and headers for a job
func (s *QueueService) getJobPayloadAndHeaders(ctx context.Context, queueMgr storage.QueueManager, resourcePath, jobID string) ([]byte, map[string]string, error) {
	// Get job metadata to find the log position
	job, err := queueMgr.GetInFlight(ctx, resourcePath, jobID)
	if err != nil {
		// Try to get from ready heap - we'd need a method for this
		// For now, use GetJobPayload and read headers separately
		payload, err := queueMgr.GetJobPayload(ctx, resourcePath, jobID)
		if err != nil {
			return nil, nil, err
		}
		// Headers not available without reading full message
		return payload, make(map[string]string), nil
	}

	// Read full message from log to get headers
	payload, headers, err := s.readJobMessageFromLog(ctx, job.PayloadPos.File, job.PayloadPos.Offset, jobID)
	if err != nil {
		// Fallback to GetJobPayload
		payload, err = queueMgr.GetJobPayload(ctx, resourcePath, jobID)
		if err != nil {
			return nil, nil, err
		}
		return payload, make(map[string]string), nil
	}

	return payload, headers, nil
}

// readJobMessageFromLog reads the full job message from log segment
func (s *QueueService) readJobMessageFromLog(ctx context.Context, filePath string, offset int64, jobID string) ([]byte, map[string]string, error) {
	reader, err := log.NewSegmentReader(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer func() {
		_ = reader.Close() // Ignore close errors in defer
	}()

	// Read entries until we find the one at the correct offset
	for {
		data, entryOffset, readErr := reader.ReadEntry()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, nil, fmt.Errorf("failed to read entry: %w", readErr)
		}

		// Check if this is the entry we're looking for
		if entryOffset == offset {
			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				return nil, nil, fmt.Errorf("failed to decode message: %w", decodeErr)
			}

			// Verify job ID matches
			if msg.ID != jobID {
				return nil, nil, fmt.Errorf("job ID mismatch: expected %s, got %s", jobID, msg.ID)
			}

			return msg.Payload, msg.Headers, nil
		}

		// If we've passed the offset, the entry wasn't found
		if entryOffset > offset {
			return nil, nil, fmt.Errorf("job entry not found at offset %d", offset)
		}
	}

	return nil, nil, fmt.Errorf("job entry not found")
}

// mapQueueError maps storage queue errors to gRPC status codes
func (s *QueueService) mapQueueError(err error, resourcePath string) error {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case queues.QueueNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case queues.JobNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case queues.InvalidVisibilityTimeoutError:
		return status.Error(codes.InvalidArgument, e.Error())
	case queues.EnqueueError:
		return status.Error(codes.Internal, e.Error())
	case queues.NACKError:
		return status.Error(codes.Internal, e.Error())
	case queues.MaxAttemptsExceededError:
		return status.Error(codes.FailedPrecondition, e.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// SetRetryPolicy sets the retry policy for a queue
func (s *QueueService) SetRetryPolicy(
	ctx context.Context,
	req *flowmeshpb.SetRetryPolicyRequest,
) (*flowmeshpb.SetRetryPolicyResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.Policy == nil {
		return nil, status.Error(codes.InvalidArgument, "policy is required")
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

	// Validate retry policy
	if err := validation.ValidateRetryPolicy(req.Policy); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Convert proto RetryPolicy to storage RetryPolicy
	policy := queues.RetryPolicy{
		MaxAttempts:       req.Policy.MaxAttempts,
		InitialBackoff:    time.Duration(req.Policy.InitialBackoffSeconds) * time.Second,
		MaxBackoff:        time.Duration(req.Policy.MaxBackoffSeconds) * time.Second,
		BackoffMultiplier: req.Policy.BackoffMultiplier,
		BackoffStrategy:   queues.BackoffStrategy(req.Policy.BackoffStrategy),
	}

	// Set retry policy
	queueMgr := s.storage.QueueManager()
	if err := queueMgr.SetRetryPolicy(ctx, resourcePath, policy); err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	return &flowmeshpb.SetRetryPolicyResponse{
		Status: &flowmeshpb.Status{Code: int32(codes.OK)},
	}, nil
}

// GetRetryPolicy gets the retry policy for a queue
func (s *QueueService) GetRetryPolicy(
	ctx context.Context,
	req *flowmeshpb.GetRetryPolicyRequest,
) (*flowmeshpb.GetRetryPolicyResponse, error) {
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

	// Get retry policy
	queueMgr := s.storage.QueueManager()
	policy, err := queueMgr.GetRetryPolicy(ctx, resourcePath)
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	// Convert storage RetryPolicy to proto RetryPolicy
	protoPolicy := &flowmeshpb.RetryPolicy{
		MaxAttempts:           policy.MaxAttempts,
		InitialBackoffSeconds: int64(policy.InitialBackoff.Seconds()),
		MaxBackoffSeconds:     int64(policy.MaxBackoff.Seconds()),
		BackoffMultiplier:     policy.BackoffMultiplier,
		BackoffStrategy:       string(policy.BackoffStrategy),
	}

	return &flowmeshpb.GetRetryPolicyResponse{
		Status: &flowmeshpb.Status{Code: int32(codes.OK)},
		Policy: protoPolicy,
	}, nil
}

// ListDLQJobs lists jobs in the dead-letter queue
func (s *QueueService) ListDLQJobs(ctx context.Context, req *flowmeshpb.ListDLQJobsRequest) (*flowmeshpb.ListDLQJobsResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.MaxJobs <= 0 {
		req.MaxJobs = 100 // Default limit
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

	// List DLQ jobs
	queueMgr := s.storage.QueueManager()
	dlqJobs, err := queueMgr.ListDLQJobs(ctx, resourcePath, int(req.MaxJobs))
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	// Get DLQ path to read jobs from
	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	// Convert QueueJob to proto Job
	protoJobs := make([]*flowmeshpb.Job, 0, len(dlqJobs))
	for _, job := range dlqJobs {
		payload, err := queueMgr.GetJobPayload(ctx, dlqPath, job.ID)
		if err != nil {
			s.log.Warn().Err(err).Str("job_id", job.ID).Msg("Failed to get job payload for DLQ listing")
			continue
		}

		protoJobs = append(protoJobs, &flowmeshpb.Job{
			Id:        job.ID,
			Seq:       job.Seq,
			Payload:   payload,
			Attempts:  int32(job.Attempts),
			CreatedAt: job.CreatedAt.Unix(),
		})
	}

	return &flowmeshpb.ListDLQJobsResponse{
		Status: &flowmeshpb.Status{Code: int32(codes.OK)},
		Jobs:   protoJobs,
	}, nil
}

// ReplayDLQJob replays a job from DLQ back to the main queue
func (s *QueueService) ReplayDLQJob(ctx context.Context, req *flowmeshpb.ReplayDLQJobRequest) (*flowmeshpb.ReplayDLQJobResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
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

	// Get DLQ path
	queueMgr := s.storage.QueueManager()
	dlqPath, err := queueMgr.GetDLQPath(ctx, resourcePath)
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	// Get job payload from DLQ
	payload, err := queueMgr.GetJobPayload(ctx, dlqPath, req.JobId)
	if err != nil {
		return nil, s.mapQueueError(err, dlqPath)
	}

	// Enqueue back to main queue (reset attempts by creating new job)
	jobID, seq, err := queueMgr.Enqueue(ctx, resourcePath, payload, storage.QueueEnqueueOptions{})
	if err != nil {
		return nil, s.mapQueueError(err, resourcePath)
	}

	return &flowmeshpb.ReplayDLQJobResponse{
		Status: &flowmeshpb.Status{Code: int32(codes.OK)},
		JobId:  jobID,
		Seq:    seq,
	}, nil
}
