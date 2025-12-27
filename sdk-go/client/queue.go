package client

import (
	"context"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
)

// QueueClient provides operations for working with queues
type QueueClient struct {
	client *Client
}

// Enqueue enqueues a job to a queue and returns job ID and sequence number
func (q *QueueClient) Enqueue(ctx context.Context, tenant, namespace, queue string, payload []byte, opts ...EnqueueOption) (jobID string, seq int64, err error) {
	options := &EnqueueOptions{}
	for _, opt := range opts {
		opt(options)
	}

	req := &flowmeshpb.EnqueueRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
		Payload:      payload,
		DelaySeconds: int64(options.Delay.Seconds()),
		Headers:      options.Headers,
	}

	resp, err := q.client.queueClient.Enqueue(q.client.withAuth(ctx), req)
	if err != nil {
		return "", 0, wrapError(err, "enqueue")
	}

	return resp.JobId, resp.Seq, nil
}

// Reserve reserves a single job from the queue with visibility timeout
func (q *QueueClient) Reserve(ctx context.Context, tenant, namespace, queue string, opts ...ReserveOption) (*Job, error) {
	options := &ReserveOptions{
		VisibilityTimeout: 30 * time.Second,
		LongPollTimeout:   0,
	}
	for _, opt := range opts {
		opt(options)
	}

	req := &flowmeshpb.ReserveRequest{
		ResourcePath:             buildResourcePath(tenant, namespace, "queue", queue),
		VisibilityTimeoutSeconds: int64(options.VisibilityTimeout.Seconds()),
		LongPollTimeoutSeconds:   int64(options.LongPollTimeout.Seconds()),
	}

	resp, err := q.client.queueClient.Reserve(q.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "reserve")
	}

	if resp.Job == nil {
		return nil, nil // No job available
	}

	return JobFromProto(resp.Job), nil
}

// Receive receives multiple jobs from the queue (batch receive)
func (q *QueueClient) Receive(ctx context.Context, tenant, namespace, queue string, opts ...ReceiveOption) ([]*Job, error) {
	options := &ReceiveOptions{
		MaxJobs:           1,
		VisibilityTimeout: 30 * time.Second,
		LongPollTimeout:   0,
	}
	for _, opt := range opts {
		opt(options)
	}

	req := &flowmeshpb.ReceiveRequest{
		ResourcePath:             buildResourcePath(tenant, namespace, "queue", queue),
		MaxJobs:                  options.MaxJobs,
		VisibilityTimeoutSeconds: int64(options.VisibilityTimeout.Seconds()),
		LongPollTimeoutSeconds:   int64(options.LongPollTimeout.Seconds()),
	}

	resp, err := q.client.queueClient.Receive(q.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "receive")
	}

	jobs := make([]*Job, len(resp.Jobs))
	for i, job := range resp.Jobs {
		jobs[i] = JobFromProto(job)
	}

	return jobs, nil
}

// ACK acknowledges completion of a job
func (q *QueueClient) ACK(ctx context.Context, tenant, namespace, queue, jobID string) error {
	req := &flowmeshpb.ACKRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
		JobId:        jobID,
	}

	_, err := q.client.queueClient.ACK(q.client.withAuth(ctx), req)
	return wrapError(err, "ack")
}

// NACK negatively acknowledges a job (requeue with backoff)
func (q *QueueClient) NACK(ctx context.Context, tenant, namespace, queue, jobID string, opts ...NACKOption) error {
	options := &NACKOptions{}
	for _, opt := range opts {
		opt(options)
	}

	req := &flowmeshpb.NACKRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
		JobId:        jobID,
		DelaySeconds: int64(options.Delay.Seconds()),
	}

	_, err := q.client.queueClient.NACK(q.client.withAuth(ctx), req)
	return wrapError(err, "nack")
}

// GetStats retrieves queue statistics (depth, in-flight, age, etc.)
func (q *QueueClient) GetStats(ctx context.Context, tenant, namespace, queue string) (*QueueStats, error) {
	req := &flowmeshpb.GetQueueStatsRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
	}

	resp, err := q.client.queueClient.GetQueueStats(q.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "get queue stats")
	}

	return QueueStatsFromProto(resp.Stats), nil
}

// SetRetryPolicy sets the retry policy for a queue
func (q *QueueClient) SetRetryPolicy(ctx context.Context, tenant, namespace, queue string, policy *RetryPolicy) error {
	req := &flowmeshpb.SetRetryPolicyRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
		Policy:       policy.ToProto(),
	}

	_, err := q.client.queueClient.SetRetryPolicy(q.client.withAuth(ctx), req)
	return wrapError(err, "set retry policy")
}

// GetRetryPolicy gets the retry policy for a queue
func (q *QueueClient) GetRetryPolicy(ctx context.Context, tenant, namespace, queue string) (*RetryPolicy, error) {
	req := &flowmeshpb.GetRetryPolicyRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
	}

	resp, err := q.client.queueClient.GetRetryPolicy(q.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "get retry policy")
	}

	return RetryPolicyFromProto(resp.Policy), nil
}

// ListDLQJobs lists jobs in the dead-letter queue
func (q *QueueClient) ListDLQJobs(ctx context.Context, tenant, namespace, queue string, maxJobs int32) ([]*Job, error) {
	req := &flowmeshpb.ListDLQJobsRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
		MaxJobs:      maxJobs,
	}

	resp, err := q.client.queueClient.ListDLQJobs(q.client.withAuth(ctx), req)
	if err != nil {
		return nil, wrapError(err, "list DLQ jobs")
	}

	jobs := make([]*Job, len(resp.Jobs))
	for i, job := range resp.Jobs {
		jobs[i] = JobFromProto(job)
	}

	return jobs, nil
}

// ReplayDLQJob replays a job from DLQ back to the main queue
func (q *QueueClient) ReplayDLQJob(ctx context.Context, tenant, namespace, queue, jobID string) (newJobID string, seq int64, err error) {
	req := &flowmeshpb.ReplayDLQJobRequest{
		ResourcePath: buildResourcePath(tenant, namespace, "queue", queue),
		JobId:        jobID,
	}

	resp, err := q.client.queueClient.ReplayDLQJob(q.client.withAuth(ctx), req)
	if err != nil {
		return "", 0, wrapError(err, "replay DLQ job")
	}

	return resp.JobId, resp.Seq, nil
}
