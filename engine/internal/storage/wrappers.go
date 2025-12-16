package storage

import (
	"context"
	"time"

	"github.com/flowmesh/engine/internal/storage/consumers"
	"github.com/flowmesh/engine/internal/storage/kv"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/queues"
	"github.com/flowmesh/engine/internal/storage/replay"
	"github.com/flowmesh/engine/internal/storage/schema"
	"github.com/flowmesh/engine/internal/storage/streams"
)

// streamManagerWrapper wraps streams.Manager to implement StreamManager interface
type streamManagerWrapper struct {
	*streams.Manager
}

// WriteEvents implements StreamWriter interface
func (w *streamManagerWrapper) WriteEvents(ctx context.Context, resourcePath string, events []StreamEvent) ([]int64, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Convert StreamEvent to streams.Event
	streamEvents := make([]streams.Event, len(events))
	for i, e := range events {
		streamEvents[i] = streams.Event{
			Payload: e.Payload,
			Headers: e.Headers,
		}
	}

	return w.Manager.WriteEvents(ctx, resourcePath, streamEvents)
}

// ReadFromOffset implements StreamReader interface
func (w *streamManagerWrapper) ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.ReadFromOffset(ctx, resourcePath, partition, offset, maxMessages)
}

// GetLatestOffset implements StreamReader interface
func (w *streamManagerWrapper) GetLatestOffset(ctx context.Context, resourcePath string, partition int32) (int64, error) {
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
	}
	return w.Manager.GetLatestOffset(resourcePath, partition)
}

// InitializeStream implements StreamManager interface
func (w *streamManagerWrapper) InitializeStream(ctx context.Context, resourcePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.InitializeStream(resourcePath)
}

// Start implements Lifecycle interface
func (w *streamManagerWrapper) Start(ctx context.Context) error {
	return nil // Stream manager doesn't need explicit startup
}

// Stop implements Lifecycle interface
func (w *streamManagerWrapper) Stop(ctx context.Context) error {
	return w.Manager.SaveIndexes()
}

// Ready implements Lifecycle interface
func (w *streamManagerWrapper) Ready() bool {
	return true
}

// queueManagerWrapper wraps queues.Manager to implement QueueManager interface
type queueManagerWrapper struct {
	*queues.Manager
}

// Enqueue implements QueueWriter interface
func (w *queueManagerWrapper) Enqueue(ctx context.Context, resourcePath string, payload []byte, options QueueEnqueueOptions) (string, int64, error) {
	select {
	case <-ctx.Done():
		return "", 0, ctx.Err()
	default:
	}

	queueOptions := queues.EnqueueOptions{
		Delay:   options.Delay,
		Headers: options.Headers,
	}

	return w.Manager.Enqueue(ctx, resourcePath, payload, queueOptions)
}

// Reserve implements QueueReader interface
func (w *queueManagerWrapper) Reserve(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	queueOptions := queues.ReserveOptions{
		VisibilityTimeout: visibilityTimeout,
	}
	job, err := w.Manager.Reserve(ctx, resourcePath, queueOptions)
	if err != nil {
		return nil, err
	}

	return &QueueJob{
		ID:           job.ID,
		Seq:          job.Seq,
		VisibleAt:    job.VisibleAt,
		ReserveUntil: job.ReserveUntil,
		PayloadPos:   job.PayloadPos,
		Attempts:     job.Attempts,
		CreatedAt:    job.CreatedAt,
	}, nil
}

// PopReadyJob implements QueueReader interface
func (w *queueManagerWrapper) PopReadyJob(ctx context.Context, resourcePath string) (*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	job, err := w.Manager.PopReadyJob(resourcePath)
	if err != nil {
		return nil, err
	}

	if job == nil {
		return nil, nil
	}

	return &QueueJob{
		ID:           job.ID,
		Seq:          job.Seq,
		VisibleAt:    job.VisibleAt,
		ReserveUntil: job.ReserveUntil,
		PayloadPos:   job.PayloadPos,
		Attempts:     job.Attempts,
		CreatedAt:    job.CreatedAt,
	}, nil
}

// Start implements Lifecycle interface
func (w *queueManagerWrapper) Start(ctx context.Context) error {
	return w.Manager.Start(ctx)
}

// Stop implements Lifecycle interface
func (w *queueManagerWrapper) Stop(ctx context.Context) error {
	return w.Manager.Stop(ctx)
}

// Ready implements Lifecycle interface
func (w *queueManagerWrapper) Ready() bool {
	return w.Manager.Ready()
}

// InitializeQueue implements QueueManager interface
func (w *queueManagerWrapper) InitializeQueue(ctx context.Context, resourcePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.InitializeQueue(resourcePath)
}

// AddToInFlight implements QueueManager interface
func (w *queueManagerWrapper) AddToInFlight(ctx context.Context, resourcePath string, jobID string, visibilityTimeout time.Duration) (*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	job, err := w.Manager.AddToInFlight(resourcePath, jobID, visibilityTimeout)
	if err != nil {
		return nil, err
	}

	return &QueueJob{
		ID:           job.ID,
		Seq:          job.Seq,
		VisibleAt:    job.VisibleAt,
		ReserveUntil: job.ReserveUntil,
		PayloadPos:   job.PayloadPos,
		Attempts:     job.Attempts,
		CreatedAt:    job.CreatedAt,
	}, nil
}

// RemoveFromInFlight implements QueueManager interface
func (w *queueManagerWrapper) RemoveFromInFlight(ctx context.Context, resourcePath string, jobID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.RemoveFromInFlight(resourcePath, jobID)
}

// GetInFlight implements QueueManager interface
func (w *queueManagerWrapper) GetInFlight(ctx context.Context, resourcePath string, jobID string) (*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	job, err := w.Manager.GetInFlight(resourcePath, jobID)
	if err != nil {
		return nil, err
	}

	return &QueueJob{
		ID:           job.ID,
		Seq:          job.Seq,
		VisibleAt:    job.VisibleAt,
		ReserveUntil: job.ReserveUntil,
		PayloadPos:   job.PayloadPos,
		Attempts:     job.Attempts,
		CreatedAt:    job.CreatedAt,
	}, nil
}

// NACK implements QueueManager interface
func (w *queueManagerWrapper) NACK(ctx context.Context, resourcePath string, jobID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.NACK(ctx, resourcePath, jobID)
}

// NACKWithDelay implements QueueManager interface
func (w *queueManagerWrapper) NACKWithDelay(ctx context.Context, resourcePath string, jobID string, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.NACKWithDelay(ctx, resourcePath, jobID, delay)
}

// GetJobPayload implements QueueManager interface
func (w *queueManagerWrapper) GetJobPayload(ctx context.Context, resourcePath string, jobID string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.GetJobPayload(ctx, resourcePath, jobID)
}

// GetQueueStats implements QueueManager interface
func (w *queueManagerWrapper) GetQueueStats(ctx context.Context, resourcePath string) (*QueueStats, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	stats, err := w.Manager.GetQueueStats(ctx, resourcePath)
	if err != nil {
		return nil, err
	}

	return &QueueStats{
		TotalJobs:     stats.TotalJobs,
		PendingJobs:   stats.PendingJobs,
		InFlightJobs:  stats.InFlightJobs,
		CompletedJobs: stats.CompletedJobs,
		FailedJobs:    stats.FailedJobs,
		OldestJobAge:  stats.OldestJobAge,
	}, nil
}

// Receive implements QueueManager interface
func (w *queueManagerWrapper) Receive(ctx context.Context, resourcePath string, maxJobs int, options QueueReserveOptions) ([]*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	queueOptions := queues.ReserveOptions{
		VisibilityTimeout: options.VisibilityTimeout,
		LongPollTimeout:   options.LongPollTimeout,
		MaxWaitTime:       options.MaxWaitTime,
	}

	jobs, err := w.Manager.Receive(ctx, resourcePath, maxJobs, queueOptions)
	if err != nil {
		return nil, err
	}

	result := make([]*QueueJob, len(jobs))
	for i, job := range jobs {
		result[i] = &QueueJob{
			ID:           job.ID,
			Seq:          job.Seq,
			VisibleAt:    job.VisibleAt,
			ReserveUntil: job.ReserveUntil,
			PayloadPos:   job.PayloadPos,
			Attempts:     job.Attempts,
			CreatedAt:    job.CreatedAt,
		}
	}

	return result, nil
}

// SetRetryPolicy implements QueueManager interface
func (w *queueManagerWrapper) SetRetryPolicy(ctx context.Context, resourcePath string, policy queues.RetryPolicy) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.SetRetryPolicy(ctx, resourcePath, policy)
}

// GetRetryPolicy implements QueueManager interface
func (w *queueManagerWrapper) GetRetryPolicy(ctx context.Context, resourcePath string) (*queues.RetryPolicy, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.GetRetryPolicy(ctx, resourcePath)
}

// MoveToDLQ implements QueueManager interface
func (w *queueManagerWrapper) MoveToDLQ(ctx context.Context, resourcePath string, jobID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.MoveToDLQ(ctx, resourcePath, jobID)
}

// GetDLQPath implements QueueManager interface
func (w *queueManagerWrapper) GetDLQPath(ctx context.Context, resourcePath string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	return w.Manager.GetDLQPath(ctx, resourcePath)
}

// ListDLQJobs implements QueueManager interface
func (w *queueManagerWrapper) ListDLQJobs(ctx context.Context, resourcePath string, maxJobs int) ([]*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	jobs, err := w.Manager.ListDLQJobs(ctx, resourcePath, maxJobs)
	if err != nil {
		return nil, err
	}

	result := make([]*QueueJob, len(jobs))
	for i, job := range jobs {
		result[i] = &QueueJob{
			ID:           job.ID,
			Seq:          job.Seq,
			VisibleAt:    job.VisibleAt,
			ReserveUntil: job.ReserveUntil,
			PayloadPos:   job.PayloadPos,
			Attempts:     job.Attempts,
			CreatedAt:    job.CreatedAt,
		}
	}

	return result, nil
}

// kvManagerWrapper wraps kv.Manager to implement KVManager interface
type kvManagerWrapper struct {
	*kv.Manager
}

// Ensure kvManagerWrapper implements KVManager interface
var _ KVManager = (*kvManagerWrapper)(nil)

// Start implements Lifecycle interface
func (w *kvManagerWrapper) Start(ctx context.Context) error {
	return w.Manager.Start(ctx)
}

// Stop implements Lifecycle interface
func (w *kvManagerWrapper) Stop(ctx context.Context) error {
	return w.Manager.Stop(ctx)
}

// Ready implements Lifecycle interface
func (w *kvManagerWrapper) Ready() bool {
	return w.Manager.Ready()
}

// Set implements KVManager interface
func (w *kvManagerWrapper) Set(ctx context.Context, resourcePath, key string, value []byte, options KVSetOptions) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	kvOptions := kv.SetOptions{
		TTL: options.TTL,
	}
	return w.Manager.Set(ctx, resourcePath, key, value, kvOptions)
}

// Get implements KVManager interface
func (w *kvManagerWrapper) Get(ctx context.Context, resourcePath, key string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.Get(ctx, resourcePath, key)
}

// Delete implements KVManager interface
func (w *kvManagerWrapper) Delete(ctx context.Context, resourcePath, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.Delete(ctx, resourcePath, key)
}

// Exists implements KVManager interface
func (w *kvManagerWrapper) Exists(ctx context.Context, resourcePath, key string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	return w.Manager.Exists(ctx, resourcePath, key)
}

// ListKeys implements KVManager interface
func (w *kvManagerWrapper) ListKeys(ctx context.Context, resourcePath, prefix string) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.ListKeys(ctx, resourcePath, prefix)
}

// InitializeKVStore implements KVManager interface
func (w *kvManagerWrapper) InitializeKVStore(ctx context.Context, resourcePath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.InitializeKVStore(ctx, resourcePath)
}

// consumerGroupManagerWrapper wraps consumers.Manager to implement ConsumerGroupManager interface
type consumerGroupManagerWrapper struct {
	*consumers.Manager
}

// Ensure consumerGroupManagerWrapper implements ConsumerGroupManager interface
var _ ConsumerGroupManager = (*consumerGroupManagerWrapper)(nil)

// Start implements Lifecycle interface
func (w *consumerGroupManagerWrapper) Start(ctx context.Context) error {
	return w.Manager.Start(ctx)
}

// Stop implements Lifecycle interface
func (w *consumerGroupManagerWrapper) Stop(ctx context.Context) error {
	return w.Manager.Stop(ctx)
}

// Ready implements Lifecycle interface
func (w *consumerGroupManagerWrapper) Ready() bool {
	return w.Manager.Ready()
}

// CommitOffset implements ConsumerGroupManager interface
func (w *consumerGroupManagerWrapper) CommitOffset(ctx context.Context, stream, group string, partition int32, offset int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.CommitOffset(ctx, stream, group, partition, offset)
}

// GetCommittedOffset implements ConsumerGroupManager interface
func (w *consumerGroupManagerWrapper) GetCommittedOffset(ctx context.Context, stream, group string, partition int32) (int64, error) {
	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	default:
	}
	return w.Manager.GetCommittedOffset(ctx, stream, group, partition)
}

// GetConsumerGroupState implements ConsumerGroupManager interface
func (w *consumerGroupManagerWrapper) GetConsumerGroupState(ctx context.Context, stream, group string, partition int32) (*ConsumerGroupState, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	state, err := w.Manager.GetConsumerGroupState(ctx, stream, group, partition)
	if err != nil {
		return nil, err
	}
	// Convert from consumers.ConsumerGroupState to interface ConsumerGroupState
	return &ConsumerGroupState{
		Stream:          state.Stream,
		Group:           state.Group,
		Partition:       state.Partition,
		CommittedOffset: state.CommittedOffset,
		LatestOffset:    state.LatestOffset,
		Lag:             state.Lag,
	}, nil
}

// CalculateLag implements ConsumerGroupManager interface
func (w *consumerGroupManagerWrapper) CalculateLag(ctx context.Context, stream, group string, partition int32) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	return w.Manager.CalculateLag(ctx, stream, group, partition)
}

// ListConsumerGroups implements ConsumerGroupManager interface
func (w *consumerGroupManagerWrapper) ListConsumerGroups(ctx context.Context, stream string) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.ListConsumerGroups(ctx, stream)
}

// DeleteConsumerGroup implements ConsumerGroupManager interface
func (w *consumerGroupManagerWrapper) DeleteConsumerGroup(ctx context.Context, stream, group string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return w.Manager.DeleteConsumerGroup(ctx, stream, group)
}

// schemaRegistryWrapper wraps schema.Registry to implement SchemaRegistry interface
type schemaRegistryWrapper struct {
	*schema.Registry
}

// Ensure schemaRegistryWrapper implements SchemaRegistry interface
var _ SchemaRegistry = (*schemaRegistryWrapper)(nil)

// Start implements Lifecycle interface
func (w *schemaRegistryWrapper) Start(ctx context.Context) error {
	return nil // Schema registry doesn't need explicit startup
}

// Stop implements Lifecycle interface
func (w *schemaRegistryWrapper) Stop(ctx context.Context) error {
	return nil // Schema registry cleanup if needed
}

// Ready implements Lifecycle interface
func (w *schemaRegistryWrapper) Ready() bool {
	return true
}

// replayManagerWrapper wraps replay.Manager to implement ReplayManager interface
type replayManagerWrapper struct {
	*replay.Manager
}

// Ensure replayManagerWrapper implements ReplayManager interface
var _ ReplayManager = (*replayManagerWrapper)(nil)

// CreateSession implements ReplayManager interface
func (w *replayManagerWrapper) CreateSession(ctx context.Context, stream string, startOffset int64, startTime *time.Time, endOffset *int64, endTime *time.Time, sandboxGroup string) (*ReplaySession, error) {
	session, err := w.Manager.CreateSession(ctx, stream, startOffset, startTime, endOffset, endTime, sandboxGroup)
	if err != nil {
		return nil, err
	}

	return &ReplaySession{
		ID:                   session.ID,
		Stream:               session.Stream,
		Partition:            session.Partition,
		StartOffset:          session.StartOffset,
		StartTime:            session.StartTime,
		EndOffset:            session.EndOffset,
		EndTime:              session.EndTime,
		SandboxConsumerGroup: session.SandboxConsumerGroup,
		Status:               string(session.Status),
		CreatedAt:            session.CreatedAt,
		UpdatedAt:            session.UpdatedAt,
	}, nil
}

// GetSession implements ReplayManager interface
func (w *replayManagerWrapper) GetSession(ctx context.Context, sessionID string) (*ReplaySession, error) {
	session, err := w.Manager.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	return &ReplaySession{
		ID:                   session.ID,
		Stream:               session.Stream,
		Partition:            session.Partition,
		StartOffset:          session.StartOffset,
		StartTime:            session.StartTime,
		EndOffset:            session.EndOffset,
		EndTime:              session.EndTime,
		SandboxConsumerGroup: session.SandboxConsumerGroup,
		Status:               string(session.Status),
		CreatedAt:            session.CreatedAt,
		UpdatedAt:            session.UpdatedAt,
	}, nil
}

// ListSessions implements ReplayManager interface
func (w *replayManagerWrapper) ListSessions(ctx context.Context, stream string) ([]*ReplaySession, error) {
	sessions, err := w.Manager.ListSessions(ctx, stream)
	if err != nil {
		return nil, err
	}

	result := make([]*ReplaySession, len(sessions))
	for i, session := range sessions {
		result[i] = &ReplaySession{
			ID:                   session.ID,
			Stream:               session.Stream,
			Partition:            session.Partition,
			StartOffset:          session.StartOffset,
			StartTime:            session.StartTime,
			EndOffset:            session.EndOffset,
			EndTime:              session.EndTime,
			SandboxConsumerGroup: session.SandboxConsumerGroup,
			Status:               string(session.Status),
			CreatedAt:            session.CreatedAt,
			UpdatedAt:            session.UpdatedAt,
		}
	}

	return result, nil
}

// GetReplayProgress implements ReplayManager interface
func (w *replayManagerWrapper) GetReplayProgress(ctx context.Context, sessionID string) (*ReplayProgress, error) {
	progress, err := w.Manager.GetReplayProgress(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	return &ReplayProgress{
		CurrentOffset:    progress.CurrentOffset,
		MessagesReplayed: progress.MessagesReplayed,
		Errors:           progress.Errors,
		StartedAt:        progress.StartedAt,
		PausedAt:         progress.PausedAt,
		CompletedAt:      progress.CompletedAt,
	}, nil
}

// Start implements Lifecycle interface
func (w *replayManagerWrapper) Start(ctx context.Context) error {
	return w.Manager.Start(ctx)
}

// Stop implements Lifecycle interface
func (w *replayManagerWrapper) Stop(ctx context.Context) error {
	return w.Manager.Stop(ctx)
}

// Ready implements Lifecycle interface
func (w *replayManagerWrapper) Ready() bool {
	return w.Manager.Ready()
}
