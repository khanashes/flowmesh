package storage

import (
	"context"
	"time"

	"github.com/flowmesh/engine/internal/storage/consumers"
	"github.com/flowmesh/engine/internal/storage/kv"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/queues"
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

	return w.Manager.WriteEvents(resourcePath, streamEvents)
}

// ReadFromOffset implements StreamReader interface
func (w *streamManagerWrapper) ReadFromOffset(ctx context.Context, resourcePath string, partition int32, offset int64, maxMessages int) ([]*log.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return w.Manager.ReadFromOffset(resourcePath, partition, offset, maxMessages)
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

	return w.Manager.Enqueue(resourcePath, payload, queueOptions)
}

// Reserve implements QueueReader interface
func (w *queueManagerWrapper) Reserve(ctx context.Context, resourcePath string, visibilityTimeout time.Duration) (*QueueJob, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Pop a ready job
	job, err := w.Manager.PopReadyJob(resourcePath)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, nil // No jobs available
	}

	// Add to InFlight
	jobMeta, err := w.Manager.AddToInFlight(resourcePath, job.ID, visibilityTimeout)
	if err != nil {
		return nil, err
	}

	return &QueueJob{
		ID:           jobMeta.ID,
		Seq:          jobMeta.Seq,
		VisibleAt:    jobMeta.VisibleAt,
		ReserveUntil: jobMeta.ReserveUntil,
		PayloadPos:   jobMeta.PayloadPos,
		Attempts:     jobMeta.Attempts,
		CreatedAt:    jobMeta.CreatedAt,
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

	jobMeta, err := w.Manager.AddToInFlight(resourcePath, jobID, visibilityTimeout)
	if err != nil {
		return nil, err
	}

	return &QueueJob{
		ID:           jobMeta.ID,
		Seq:          jobMeta.Seq,
		VisibleAt:    jobMeta.VisibleAt,
		ReserveUntil: jobMeta.ReserveUntil,
		PayloadPos:   jobMeta.PayloadPos,
		Attempts:     jobMeta.Attempts,
		CreatedAt:    jobMeta.CreatedAt,
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

	jobMeta, err := w.Manager.GetInFlight(resourcePath, jobID)
	if err != nil {
		return nil, err
	}

	return &QueueJob{
		ID:           jobMeta.ID,
		Seq:          jobMeta.Seq,
		VisibleAt:    jobMeta.VisibleAt,
		ReserveUntil: jobMeta.ReserveUntil,
		PayloadPos:   jobMeta.PayloadPos,
		Attempts:     jobMeta.Attempts,
		CreatedAt:    jobMeta.CreatedAt,
	}, nil
}

// Start implements Lifecycle interface
func (w *queueManagerWrapper) Start(ctx context.Context) error {
	return nil // Queue manager doesn't need explicit startup
}

// Stop implements Lifecycle interface
func (w *queueManagerWrapper) Stop(ctx context.Context) error {
	return nil // Queue manager cleanup if needed
}

// Ready implements Lifecycle interface
func (w *queueManagerWrapper) Ready() bool {
	return true
}

// kvManagerWrapper wraps kv.Manager to implement KVManager interface
type kvManagerWrapper struct {
	*kv.Manager
}

// Ensure kvManagerWrapper implements KVManager interface
var _ KVManager = (*kvManagerWrapper)(nil)

// Set implements KVManager interface
func (w *kvManagerWrapper) Set(ctx context.Context, resourcePath, key string, value []byte, options KVSetOptions) error {
	kvOptions := kv.SetOptions{
		TTL: options.TTL,
	}
	return w.Manager.Set(ctx, resourcePath, key, value, kvOptions)
}

// Get implements KVManager interface
func (w *kvManagerWrapper) Get(ctx context.Context, resourcePath, key string) ([]byte, error) {
	return w.Manager.Get(ctx, resourcePath, key)
}

// Delete implements KVManager interface
func (w *kvManagerWrapper) Delete(ctx context.Context, resourcePath, key string) error {
	return w.Manager.Delete(ctx, resourcePath, key)
}

// Exists implements KVManager interface
func (w *kvManagerWrapper) Exists(ctx context.Context, resourcePath, key string) (bool, error) {
	return w.Manager.Exists(ctx, resourcePath, key)
}

// ListKeys implements KVManager interface
func (w *kvManagerWrapper) ListKeys(ctx context.Context, resourcePath, prefix string) ([]string, error) {
	return w.Manager.ListKeys(ctx, resourcePath, prefix)
}

// InitializeKVStore implements KVManager interface
func (w *kvManagerWrapper) InitializeKVStore(ctx context.Context, resourcePath string) error {
	return w.Manager.InitializeKVStore(ctx, resourcePath)
}

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

// consumerGroupManagerWrapper wraps consumers.Manager to implement ConsumerGroupManager interface
type consumerGroupManagerWrapper struct {
	*consumers.Manager
}

// Ensure consumerGroupManagerWrapper implements ConsumerGroupManager interface
var _ ConsumerGroupManager = (*consumerGroupManagerWrapper)(nil)

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
	// Convert consumers.ConsumerGroupState to storage.ConsumerGroupState
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
		return -1, ctx.Err()
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
