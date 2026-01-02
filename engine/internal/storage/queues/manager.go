package queues

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/filter"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/metrics"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/flowmesh/engine/internal/storage/schema"
	"github.com/flowmesh/engine/internal/tracing"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
)

// QueueState represents the state of a queue
type QueueState struct {
	ResourcePath string
	NextSeq      int64
	ReadyHeap    *JobMinHeap
	InFlight     map[string]*JobMetadata
	mu           sync.RWMutex
}

// Manager manages queue operations
type Manager struct {
	metaStore      *metastore.Store
	logManager     *log.Manager
	metadataDir    string
	schemaRegistry *schema.Registry
	validator      *schema.Validator
	queueStates    map[string]*QueueState
	workers        map[string]map[string]*WorkerInfo // queuePath -> workerID -> WorkerInfo
	retryPolicies  map[string]*RetryPolicy           // queuePath -> RetryPolicy
	metrics        *metrics.QueueMetrics             // Optional metrics collector
	log            zerolog.Logger
	mu             sync.RWMutex
	scheduler      *Scheduler
}

// NewManager creates a new queue manager
func NewManager(metaStore *metastore.Store, logManager *log.Manager, metadataDir string, queueMetrics ...*metrics.QueueMetrics) *Manager {
	var qm *metrics.QueueMetrics
	if len(queueMetrics) > 0 && queueMetrics[0] != nil {
		qm = queueMetrics[0]
	}
	mgr := &Manager{
		metaStore:      metaStore,
		logManager:     logManager,
		metadataDir:    metadataDir,
		schemaRegistry: schema.NewRegistry(metaStore),
		validator:      schema.NewValidator(),
		queueStates:    make(map[string]*QueueState),
		workers:        make(map[string]map[string]*WorkerInfo),
		retryPolicies:  make(map[string]*RetryPolicy),
		metrics:        qm,
		log:            logger.WithComponent("queues"),
		scheduler:      NewScheduler(metaStore, logManager),
	}
	mgr.scheduler.SetQueueManager(mgr)
	return mgr
}

// Start starts the queue manager and scheduler
func (m *Manager) Start(ctx context.Context) error {
	return m.scheduler.Start(ctx)
}

// Stop stops the queue manager and scheduler
func (m *Manager) Stop(ctx context.Context) error {
	return m.scheduler.Stop(ctx)
}

// Ready returns true if the queue manager is ready
func (m *Manager) Ready() bool {
	return true
}

// getOrCreateQueueState gets or creates queue state for a resource
func (m *Manager) getOrCreateQueueState(resourcePath string) *QueueState {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state, exists := m.queueStates[resourcePath]; exists {
		return state
	}

	state := &QueueState{
		ResourcePath: resourcePath,
		NextSeq:      0,
		ReadyHeap:    NewJobMinHeap(),
		InFlight:     make(map[string]*JobMetadata),
	}
	m.queueStates[resourcePath] = state
	return state
}

// Enqueue enqueues a job to a queue and returns job ID and sequence number
func (m *Manager) Enqueue(ctx context.Context, resourcePath string, payload []byte, options EnqueueOptions) (string, int64, error) {
	startTime := time.Now()

	// Start tracing span
	ctx, span := StartEnqueueSpan(ctx, resourcePath)
	defer span.End()

	// Validate queue exists and get config
	config, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return "", 0, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: err}
	}

	// Validate payload against schema if schema is configured
	if config.Schema != nil {
		// Get schema (use latest if version is 0)
		var schemaDef *schema.Schema
		if config.Schema.Version > 0 {
			schemaDef, err = m.schemaRegistry.GetSchema(ctx, config.Tenant, config.Schema.ID, config.Schema.Version)
		} else {
			schemaDef, err = m.schemaRegistry.GetLatestSchema(ctx, config.Tenant, config.Schema.ID)
		}
		if err != nil {
			span.RecordError(err)
			return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to get schema: %w", err)}
		}

		// Validate payload
		if err := m.validator.Validate(payload, schemaDef.Definition); err != nil {
			span.RecordError(err)
			return "", 0, EnqueueError{
				ResourcePath: resourcePath,
				Err:          fmt.Errorf("payload validation failed: %w", err),
			}
		}
	}

	// Get or create queue state
	state := m.getOrCreateQueueState(resourcePath)

	// Partition 0 for MVP
	partition := int32(0)

	state.mu.Lock()
	defer state.mu.Unlock()

	// Assign next sequence number
	seq := state.NextSeq
	state.NextSeq++

	// Generate unique job ID
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to generate ID: %w", err)}
	}
	jobID := hex.EncodeToString(idBytes)

	// Calculate VisibleAt
	now := time.Now()
	visibleAt := now.Add(options.Delay)

	// Get active segment writer
	writer, err := m.logManager.OpenSegment(resourcePath, partition)
	if err != nil {
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: err}
	}

	// Get file position before writing
	fileOffset := writer.Offset()

	// Determine schema version
	schemaVersion := int32(0)
	if config.Schema != nil {
		if config.Schema.Version > 0 {
			schemaVersion = config.Schema.Version
		} else {
			// Get latest version
			latestSchema, err := m.schemaRegistry.GetLatestSchema(ctx, config.Tenant, config.Schema.ID)
			if err == nil {
				schemaVersion = latestSchema.Version
			}
		}
	}

	// Ensure headers map exists
	if options.Headers == nil {
		options.Headers = make(map[string]string)
	}

	// Inject trace context into message headers
	tracing.InjectToHeaders(ctx, options.Headers)

	// Create message
	msg := &log.Message{
		ID:            jobID,
		ResourcePath:  resourcePath,
		Partition:     partition,
		Offset:        0, // Not used for queues
		Seq:           seq,
		Type:          log.MessageTypeQueue,
		Payload:       payload,
		Headers:       options.Headers,
		CreatedAt:     now,
		VisibleAt:     visibleAt,
		Attempts:      0,
		SchemaVersion: schemaVersion,
	}

	// Encode message
	encoded, err := log.EncodeMessage(msg)
	if err != nil {
		span.RecordError(err)
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to encode message: %w", err)}
	}

	// Write to segment
	if err := writer.WriteEntry(encoded); err != nil {
		span.RecordError(err)
		return "", 0, EnqueueError{ResourcePath: resourcePath, Err: fmt.Errorf("failed to write entry: %w", err)}
	}

	// Set span attributes
	span.SetAttributes(
		attribute.String("queue.job_id", jobID),
		attribute.Int64("queue.seq", seq),
	)

	// Get segment path for PayloadPos
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err == nil && len(segments) > 0 {
		// Use the last segment (most recent)
		lastSegment := segments[len(segments)-1]

		// Create job metadata
		jobMetadata := &JobMetadata{
			ID:           jobID,
			Seq:          seq,
			VisibleAt:    visibleAt,
			ReserveUntil: time.Time{}, // Set when reserved
			PayloadPos: log.FilePointer{
				File:   lastSegment.Path,
				Offset: fileOffset,
			},
			Attempts:  0,
			CreatedAt: now,
		}

		// Push job into ReadyHeap
		state.ReadyHeap.PushJob(jobMetadata)
	}

	// Flush segment
	if err := writer.Flush(); err != nil {
		m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to flush segment after enqueue")
	}

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Int64("seq", seq).
		Time("visible_at", visibleAt).
		Msg("Job enqueued")

	// Record metrics
	if m.metrics != nil {
		duration := time.Since(startTime)
		m.metrics.RecordEnqueue(config.Tenant, config.Namespace, config.Name, duration)
	}

	return jobID, seq, nil
}

// AddToInFlight moves a job from ReadyHeap to InFlight
func (m *Manager) AddToInFlight(resourcePath string, jobID string, visibilityTimeout time.Duration) (*JobMetadata, error) {
	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return nil, err
	}

	// Validate visibility timeout
	if visibilityTimeout <= 0 {
		return nil, InvalidVisibilityTimeoutError{
			Timeout: visibilityTimeout,
			Reason:  "must be greater than zero",
		}
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Check if already in flight
	if job, exists := state.InFlight[jobID]; exists {
		return job, nil
	}

	// Find job in ReadyHeap
	var job *JobMetadata
	allJobs := state.ReadyHeap.GetAll()
	for _, j := range allJobs {
		if j.ID == jobID {
			job = j
			break
		}
	}

	if job == nil {
		return nil, JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
	}

	// Check if job is visible
	now := time.Now()
	if job.VisibleAt.After(now) {
		return nil, fmt.Errorf("job %s is not yet visible (visible at %v)", jobID, job.VisibleAt) //nolint:goerr113
	}

	// Remove from ReadyHeap
	state.ReadyHeap.Remove(jobID)

	// Set ReserveUntil
	job.ReserveUntil = now.Add(visibilityTimeout)
	job.Attempts++

	// Add to InFlight
	state.InFlight[jobID] = job

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Time("reserve_until", job.ReserveUntil).
		Msg("Job moved to InFlight")

	return job, nil
}

// ACK acknowledges a job (removes from InFlight)
func (m *Manager) ACK(ctx context.Context, resourcePath string, jobID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Start tracing span
	_, span := StartACKSpan(ctx, resourcePath, jobID)
	defer span.End()

	err := m.RemoveFromInFlight(resourcePath, jobID)
	if err != nil {
		span.RecordError(err)
	} else {
		if m.metrics != nil {
			// Get config for tenant/namespace/queue labels
			if config, configErr := m.metaStore.GetResource(resourcePath); configErr == nil {
				m.metrics.RecordJobCompleted(config.Tenant, config.Namespace, config.Name)
			}
		}
	}
	return err
}

// RemoveFromInFlight removes a job from InFlight (used for ACK)
func (m *Manager) RemoveFromInFlight(resourcePath string, jobID string) error {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Check if job exists in InFlight (idempotent)
	if _, exists := state.InFlight[jobID]; !exists {
		// Already ACKed, return nil for idempotency
		return nil
	}

	// Persist ACK to log
	if err := m.writeACKEntry(resourcePath, jobID); err != nil {
		return fmt.Errorf("failed to persist ACK: %w", err)
	}

	delete(state.InFlight, jobID)

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Msg("Job removed from InFlight (ACKed)")

	return nil
}

// writeACKEntry writes an ACK entry to the log
func (m *Manager) writeACKEntry(resourcePath string, jobID string) error {
	// Partition 0 for MVP
	partition := int32(0)

	writer, err := m.logManager.OpenSegment(resourcePath, partition)
	if err != nil {
		return err
	}

	msg := &log.Message{
		ID:           jobID,
		ResourcePath: resourcePath,
		Type:         log.MessageTypeQueueAck,
		CreatedAt:    time.Now(),
		Payload:      []byte{}, // Empty payload for ACK
	}

	encoded, err := log.EncodeMessage(msg)
	if err != nil {
		return err
	}

	if err := writer.WriteEntry(encoded); err != nil {
		return err
	}

	// Flush based on policy (handled by writer, but we explicit flush to be sure)
	// Actually writer.WriteEntry handles policy. But we might want to force flush for ACKs?
	// For now rely on policy.
	return nil
}

// NACK negatively acknowledges a job (requeues with backoff)
func (m *Manager) NACK(ctx context.Context, resourcePath string, jobID string) error {
	// Start tracing span
	ctx, span := StartNACKSpan(ctx, resourcePath, jobID)
	defer span.End()

	err := m.NACKWithDelay(ctx, resourcePath, jobID, 0)
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// NACKWithDelay negatively acknowledges a job with explicit delay
func (m *Manager) NACKWithDelay(ctx context.Context, resourcePath string, jobID string, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	policy := m.retryPolicies[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()

	// Get job from InFlight
	job, exists := state.InFlight[jobID]
	if !exists {
		state.mu.Unlock()
		return JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
	}

	// Use default policy if not set
	if policy == nil {
		defaultPolicy := DefaultRetryPolicy()
		policy = &defaultPolicy
	}

	// Check max attempts
	if policy.MaxAttempts > 0 && job.Attempts >= policy.MaxAttempts {
		// Max attempts exceeded - move to DLQ
		// Keep job in InFlight for MoveToDLQ to find it
		state.mu.Unlock() // Release lock before calling MoveToDLQ (it will acquire its own locks)
		err := m.MoveToDLQ(ctx, resourcePath, jobID)
		if err != nil {
			// If MoveToDLQ fails, log warning but still return MaxAttemptsExceededError
			// The job exceeded max attempts regardless of DLQ move success
			m.log.Warn().
				Err(err).
				Str("resource", resourcePath).
				Str("job_id", jobID).
				Msg("Max attempts exceeded but failed to move to DLQ")
			// Ensure job is removed from InFlight
			state.mu.Lock()
			delete(state.InFlight, jobID)
			state.mu.Unlock()
		}
		// Record failed job metric
		if m.metrics != nil {
			if config, configErr := m.metaStore.GetResource(resourcePath); configErr == nil {
				m.metrics.RecordJobFailed(config.Tenant, config.Namespace, config.Name)
			}
		}
		return MaxAttemptsExceededError{
			JobID:        jobID,
			ResourcePath: resourcePath,
			Attempts:     job.Attempts,
			MaxAttempts:  policy.MaxAttempts,
		}
	}

	// Continue with normal NACK processing (lock still held)
	defer state.mu.Unlock()

	// Calculate backoff
	if delay == 0 {
		delay = m.calculateBackoff(job.Attempts, policy)
	}

	// Remove from InFlight
	delete(state.InFlight, jobID)

	// Update job metadata
	job.VisibleAt = time.Now().Add(delay)
	job.ReserveUntil = time.Time{}
	job.Attempts++

	// Reinsert into ReadyHeap
	state.ReadyHeap.PushJob(job)

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", jobID).
		Int32("attempts", job.Attempts).
		Dur("backoff", delay).
		Msg("Job NACKed and requeued")

	return nil
}

// calculateBackoff calculates the backoff delay based on attempts and policy
func (m *Manager) calculateBackoff(attempts int32, policy *RetryPolicy) time.Duration {
	var delay time.Duration

	switch policy.BackoffStrategy {
	case BackoffStrategyFixed:
		delay = policy.InitialBackoff
	case BackoffStrategyLinear:
		delay = policy.InitialBackoff * time.Duration(attempts)
	case BackoffStrategyExponential:
		multiplier := math.Pow(policy.BackoffMultiplier, float64(attempts-1))
		delay = time.Duration(float64(policy.InitialBackoff) * multiplier)
	default:
		delay = policy.InitialBackoff
	}

	// Enforce max backoff
	if delay > policy.MaxBackoff {
		delay = policy.MaxBackoff
	}

	return delay
}

// SetRetryPolicy sets the retry policy for a queue
func (m *Manager) SetRetryPolicy(ctx context.Context, resourcePath string, policy RetryPolicy) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return QueueNotFoundError{ResourcePath: resourcePath}
		}
		return fmt.Errorf("failed to get resource: %w", err)
	}

	// Convert RetryPolicy to RetryPolicyConfig
	retryPolicyConfig := &metastore.RetryPolicyConfig{
		MaxAttempts:           policy.MaxAttempts,
		InitialBackoffSeconds: int64(policy.InitialBackoff.Seconds()),
		MaxBackoffSeconds:     int64(policy.MaxBackoff.Seconds()),
		BackoffMultiplier:     policy.BackoffMultiplier,
		BackoffStrategy:       string(policy.BackoffStrategy),
	}

	// Update resource config using updater function
	if err := m.metaStore.UpdateResource(resourcePath, func(config *metastore.ResourceConfig) error {
		config.RetryPolicy = retryPolicyConfig
		return nil
	}); err != nil {
		return fmt.Errorf("failed to update resource: %w", err)
	}

	// Update in-memory retry policy
	m.mu.Lock()
	m.retryPolicies[resourcePath] = &policy
	m.mu.Unlock()

	m.log.Info().
		Str("resource", resourcePath).
		Int32("max_attempts", policy.MaxAttempts).
		Str("strategy", string(policy.BackoffStrategy)).
		Msg("Retry policy set")

	return nil
}

// GetRetryPolicy gets the retry policy for a queue
func (m *Manager) GetRetryPolicy(ctx context.Context, resourcePath string) (*RetryPolicy, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Check in-memory cache first
	m.mu.RLock()
	if policy, exists := m.retryPolicies[resourcePath]; exists {
		m.mu.RUnlock()
		return policy, nil
	}
	m.mu.RUnlock()

	// Load from resource config
	config, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return nil, fmt.Errorf("failed to get resource: %w", err)
	}

	// Convert RetryPolicyConfig to RetryPolicy
	var policy *RetryPolicy
	if config.RetryPolicy != nil {
		policy = &RetryPolicy{
			MaxAttempts:       config.RetryPolicy.MaxAttempts,
			InitialBackoff:    time.Duration(config.RetryPolicy.InitialBackoffSeconds) * time.Second,
			MaxBackoff:        time.Duration(config.RetryPolicy.MaxBackoffSeconds) * time.Second,
			BackoffMultiplier: config.RetryPolicy.BackoffMultiplier,
			BackoffStrategy:   BackoffStrategy(config.RetryPolicy.BackoffStrategy),
		}
	} else {
		// Return default policy
		defaultPolicy := DefaultRetryPolicy()
		policy = &defaultPolicy
	}

	// Cache in memory
	m.mu.Lock()
	m.retryPolicies[resourcePath] = policy
	m.mu.Unlock()

	return policy, nil
}

// GetDLQPath gets or creates the DLQ path for a queue
func (m *Manager) GetDLQPath(ctx context.Context, resourcePath string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	// Get queue config
	config, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return "", QueueNotFoundError{ResourcePath: resourcePath}
		}
		return "", fmt.Errorf("failed to get resource: %w", err)
	}

	// Check if DLQ path is configured
	if config.DLQ != nil && config.DLQ.Enabled && config.DLQ.DLQPath != "" {
		return config.DLQ.DLQPath, nil
	}

	// Auto-generate DLQ path: {queuePath}-dlq
	dlqPath := resourcePath + "-dlq"

	// Create DLQ queue if it doesn't exist
	_, err = m.metaStore.GetResource(dlqPath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			// Create DLQ queue
			dlqConfig := &metastore.ResourceConfig{
				Tenant:     config.Tenant,
				Namespace:  config.Namespace,
				Name:       config.Name + "-dlq",
				Type:       metastore.ResourceQueue,
				Partitions: config.Partitions,
				CreatedAt:  time.Now(),
			}

			if err := dlqConfig.Validate(); err != nil {
				return "", fmt.Errorf("failed to validate DLQ config: %w", err)
			}

			if err := m.metaStore.CreateResource(dlqConfig); err != nil {
				return "", fmt.Errorf("failed to create DLQ queue: %w", err)
			}

			// Update original queue config to store DLQ path
			if config.DLQ == nil {
				config.DLQ = &metastore.DLQConfig{
					Enabled: true,
				}
			}
			config.DLQ.DLQPath = dlqPath

			if err := m.metaStore.UpdateResource(resourcePath, func(c *metastore.ResourceConfig) error {
				c.DLQ = config.DLQ
				return nil
			}); err != nil {
				m.log.Warn().Err(err).Str("resource", resourcePath).Msg("Failed to update DLQ path in config")
			}

			m.log.Info().
				Str("queue", resourcePath).
				Str("dlq", dlqPath).
				Msg("DLQ queue created")
		} else {
			return "", fmt.Errorf("failed to check DLQ existence: %w", err)
		}
	}

	return dlqPath, nil
}

// MoveToDLQ moves a job to the dead-letter queue
func (m *Manager) MoveToDLQ(ctx context.Context, resourcePath string, jobID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Get config for metrics
	config, configErr := m.metaStore.GetResource(resourcePath)
	if configErr != nil {
		if _, ok := configErr.(metastore.ResourceNotFoundError); ok {
			return QueueNotFoundError{ResourcePath: resourcePath}
		}
		return fmt.Errorf("failed to get resource config: %w", configErr)
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	job, exists := state.InFlight[jobID]
	if !exists {
		// Job might be in ReadyHeap - try to find it there
		allJobs := state.ReadyHeap.GetAll()
		for _, j := range allJobs {
			if j.ID == jobID {
				job = j
				// Remove from ReadyHeap
				state.ReadyHeap.Remove(jobID)
				break
			}
		}
		if job == nil {
			state.mu.Unlock()
			return JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
		}
	} else {
		// Remove from InFlight
		delete(state.InFlight, jobID)
	}
	state.mu.Unlock()

	// Get DLQ path
	dlqPath, err := m.GetDLQPath(ctx, resourcePath)
	if err != nil {
		return fmt.Errorf("failed to get DLQ path: %w", err)
	}

	// Read job payload and headers directly from log using the job metadata we have
	payload := []byte{}
	headers := make(map[string]string)
	reader, err := log.NewSegmentReader(job.PayloadPos.File)
	if err == nil {
		// Try to read payload and headers from log
		for {
			data, offset, readErr := reader.ReadEntry()
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				break
			}
			if offset == job.PayloadPos.Offset {
				msg, decodeErr := log.DecodeMessage(data)
				if decodeErr == nil && msg.ID == jobID {
					payload = msg.Payload
					headers = msg.Headers
					if headers == nil {
						headers = make(map[string]string)
					}
					// Add DLQ metadata
					headers["x-flowmesh-original-queue"] = resourcePath
					headers["x-flowmesh-original-job-id"] = jobID
					headers["x-flowmesh-failed-at"] = time.Now().Format(time.RFC3339)
					headers["x-flowmesh-attempts"] = fmt.Sprintf("%d", job.Attempts)
				}
				break
			}
			if offset > job.PayloadPos.Offset {
				break
			}
		}
		_ = reader.Close() // Ignore close error
	}

	if len(payload) == 0 {
		return fmt.Errorf("failed to read job payload from log")
	}

	// Enqueue to DLQ
	_, _, err = m.Enqueue(ctx, dlqPath, payload, EnqueueOptions{
		Headers: headers,
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue to DLQ: %w", err)
	}

	// Job was already removed from InFlight earlier, no need to remove again

	m.log.Info().
		Str("queue", resourcePath).
		Str("job_id", jobID).
		Str("dlq", dlqPath).
		Int32("attempts", job.Attempts).
		Msg("Job moved to DLQ")

	// Update DLQ metrics
	if m.metrics != nil {
		// Get DLQ stats to update metrics
		if dlqStats, err := m.GetQueueStats(ctx, dlqPath); err == nil {
			// Extract tenant/namespace from dlqPath (format: tenant/namespaces/namespace/queues/name-dlq)
			// For now, use the original queue's tenant/namespace
			m.metrics.UpdateDLQJobs(config.Tenant, config.Namespace, config.Name, dlqStats.PendingJobs+dlqStats.InFlightJobs)
		}
	}

	return nil
}

// ListDLQJobs lists jobs in the DLQ
func (m *Manager) ListDLQJobs(ctx context.Context, resourcePath string, maxJobs int) ([]*JobMetadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Get DLQ path
	dlqPath, err := m.GetDLQPath(ctx, resourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get DLQ path: %w", err)
	}

	// Initialize DLQ queue if not already initialized
	m.mu.RLock()
	_, exists := m.queueStates[dlqPath]
	m.mu.RUnlock()

	if !exists {
		if err := m.InitializeQueue(dlqPath); err != nil {
			return nil, fmt.Errorf("failed to initialize DLQ queue: %w", err)
		}
	}

	// Get DLQ queue state
	state := m.getOrCreateQueueState(dlqPath)

	state.mu.RLock()
	defer state.mu.RUnlock()

	// Collect jobs from ReadyHeap (need to copy to avoid modifying original)
	jobs := make([]*JobMetadata, 0, maxJobs)
	heapCopy := state.ReadyHeap.Copy()

	// Pop jobs from copy (doesn't affect original heap)
	for i := 0; i < maxJobs && heapCopy.Len() > 0; i++ {
		job := heapCopy.PopJob()
		if job != nil {
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

// requeueJob requeues a job with backoff
func (m *Manager) requeueJob(resourcePath string, job *JobMetadata, delay time.Duration) error {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Remove from InFlight if present
	delete(state.InFlight, job.ID)

	// Update job metadata
	job.VisibleAt = time.Now().Add(delay)
	job.ReserveUntil = time.Time{}
	job.Attempts++

	// Reinsert into ReadyHeap
	state.ReadyHeap.PushJob(job)

	return nil
}

// GetInFlight retrieves a job from InFlight
func (m *Manager) GetInFlight(resourcePath string, jobID string) (*JobMetadata, error) {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	job, exists := state.InFlight[jobID]
	if !exists {
		return nil, JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
	}

	return job, nil
}

// PopReadyJob pops a ready job from the heap (if visible)
func (m *Manager) PopReadyJob(resourcePath string) (*JobMetadata, error) {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	now := time.Now()

	// Peek at the top job
	job := state.ReadyHeap.Peek()
	if job == nil {
		return nil, nil // No jobs available
	}

	// Check if job is visible
	if job.VisibleAt.After(now) {
		return nil, nil // Not yet visible
	}

	// Pop the job
	popped := state.ReadyHeap.PopJob()
	if popped == nil {
		return nil, nil // No jobs available
	}
	return popped, nil
}

// InitializeQueue initializes a queue by loading state from disk
func (m *Manager) InitializeQueue(resourcePath string) error {
	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return QueueNotFoundError{ResourcePath: resourcePath}
		}
		return err
	}

	// Partition 0 for MVP
	partition := int32(0)

	// List segments
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}

	// Get or create queue state
	state := m.getOrCreateQueueState(resourcePath)

	state.mu.Lock()
	defer state.mu.Unlock()

	if len(segments) == 0 {
		// New queue, initialize empty state
		state.NextSeq = 0
		return nil
	}

	// Recover sequence from last segment
	lastSeq, err := m.recoverLastSeq(resourcePath, partition)
	if err != nil {
		return fmt.Errorf("failed to recover last sequence: %w", err)
	}

	state.NextSeq = lastSeq + 1

	// Rebuild ReadyHeap by reading all messages
	if err := m.rebuildReadyHeap(resourcePath, partition, state); err != nil {
		return fmt.Errorf("failed to rebuild ReadyHeap: %w", err)
	}

	m.log.Info().
		Str("resource", resourcePath).
		Int64("next_seq", state.NextSeq).
		Int("ready_jobs", state.ReadyHeap.Len()).
		Int("segments", len(segments)).
		Msg("Queue initialized")

	return nil
}

// Reserve reserves a job from the queue with visibility timeout
func (m *Manager) Reserve(ctx context.Context, resourcePath string, options ReserveOptions) (*JobMetadata, error) {
	startTime := time.Now()

	// Start tracing span
	ctx, span := StartReserveSpan(ctx, resourcePath)
	defer span.End()

	// Check context first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Validate queue exists
	config, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return nil, err
	}

	// Validate visibility timeout
	if options.VisibilityTimeout <= 0 {
		return nil, InvalidVisibilityTimeoutError{
			Timeout: options.VisibilityTimeout,
			Reason:  "must be greater than zero",
		}
	}

	// Use default options if not provided
	if options.VisibilityTimeout == 0 {
		options = DefaultReserveOptions()
	}

	// Try to get a job immediately
	job, err := m.PopReadyJobWithContext(ctx, resourcePath)
	if err != nil {
		return nil, err
	}

	// If no job and long polling enabled, wait
	if job == nil && options.LongPollTimeout > 0 {
		job, err = m.longPollForJob(ctx, resourcePath, options.LongPollTimeout, options.MaxWaitTime)
		if err != nil {
			return nil, err
		}
		if job == nil {
			return nil, nil // Timeout reached
		}
	}

	if job == nil {
		return nil, nil // No jobs available
	}

	// Add to InFlight (job is already popped from ReadyHeap)
	// We need to add it directly to InFlight
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	// Set ReserveUntil
	now := time.Now()
	job.ReserveUntil = now.Add(options.VisibilityTimeout)
	job.Attempts++

	// Add to InFlight
	state.InFlight[job.ID] = job

	m.log.Debug().
		Str("resource", resourcePath).
		Str("job_id", job.ID).
		Time("reserve_until", job.ReserveUntil).
		Msg("Job reserved")

	// Record metrics
	if m.metrics != nil {
		duration := time.Since(startTime)
		m.metrics.RecordReserve(config.Tenant, config.Namespace, config.Name, duration)
	}

	// Set span attributes (job is guaranteed to be non-nil at this point)
	span.SetAttributes(
		attribute.String("queue.job_id", job.ID),
		attribute.Int("queue.attempts", int(job.Attempts)),
	)

	return job, nil
}

// PopReadyJobWithContext pops a ready job with context support
func (m *Manager) PopReadyJobWithContext(ctx context.Context, resourcePath string) (*JobMetadata, error) {
	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	now := time.Now()

	// Try to get a job immediately
	state.mu.Lock()
	job := state.ReadyHeap.Peek()
	if job != nil && !job.VisibleAt.After(now) {
		popped := state.ReadyHeap.PopJob()
		state.mu.Unlock()
		return popped, nil
	}
	state.mu.Unlock()

	// No job available immediately, return nil
	return nil, nil
}

// longPollForJob waits for a job to become available
func (m *Manager) longPollForJob(ctx context.Context, resourcePath string, pollTimeout, maxWait time.Duration) (*JobMetadata, error) {
	deadline := time.Now().Add(maxWait)
	if pollTimeout > 0 && pollTimeout < maxWait {
		deadline = time.Now().Add(pollTimeout)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, nil // Timeout
			}

			// Try to get a job
			job, err := m.PopReadyJobWithContext(ctx, resourcePath)
			if err != nil {
				return nil, err
			}
			if job != nil {
				return job, nil
			}
		}
	}
}

// Peek returns a list of messages from the queue without consuming them
func (m *Manager) Peek(ctx context.Context, resourcePath string, filterStr string, limit int) ([]*log.Message, error) {
	// Start tracing span
	_, span := StartPeekSpan(ctx, resourcePath)
	defer span.End()

	// Parse filter if present
	var filterExpr filter.Expression
	if filterStr != "" {
		var err error
		filterExpr, err = filter.Parse(filterStr)
		if err != nil {
			return nil, fmt.Errorf("invalid filter expression: %w", err)
		}
	}

	// Validate limit
	if limit <= 0 {
		limit = 10 // Default
	}
	if limit > 1000 {
		limit = 1000 // Hard cap
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		// Verify resource exists in metastore
		if _, err := m.metaStore.GetResource(resourcePath); err != nil {
			if _, ok := err.(metastore.ResourceNotFoundError); ok {
				return nil, QueueNotFoundError{ResourcePath: resourcePath}
			}
			return nil, err
		}
		// If resource exists but state doesn't, it might be empty or not loaded
		// Just return empty list
		return []*log.Message{}, nil
	}

	// Get all jobs from heap (thread-safe copy)
	jobs := state.ReadyHeap.GetAll()

	// Sort jobs by VisibleAt then Seq to simulate Dequeue order
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].VisibleAt.Equal(jobs[j].VisibleAt) {
			return jobs[i].Seq < jobs[j].Seq
		}
		return jobs[i].VisibleAt.Before(jobs[j].VisibleAt)
	})

	messages := make([]*log.Message, 0, len(jobs))

	// Iterate logic
	for _, job := range jobs {
		if len(messages) >= limit {
			break
		}

		// Read message content
		reader, err := log.NewSegmentReader(job.PayloadPos.File)
		if err != nil {
			m.log.Warn().Err(err).Str("file", job.PayloadPos.File).Msg("Failed to open segment for peek")
			continue
		}

		// Seek to offset
		if _, err := reader.Seek(job.PayloadPos.Offset, io.SeekStart); err != nil {
			_ = reader.Close() // Ignore close error
			m.log.Warn().Err(err).Msg("Failed to seek to message offset")
			continue
		}

		data, _, err := reader.ReadEntry()
		_ = reader.Close() // Close immediately to avoid leaking handles in loop
		if err != nil {
			m.log.Warn().Err(err).Msg("Failed to read message entry")
			continue
		}

		msg, err := log.DecodeMessage(data)
		if err != nil {
			m.log.Warn().Err(err).Msg("Failed to decode message")
			continue
		}

		// Apply filter
		if filterExpr != nil {
			matchCtx := filter.Context{
				"headers":    msg.Headers,
				"partition":  msg.Partition,
				"seq":        msg.Seq,
				"visible_at": job.VisibleAt.Unix(),
				"attempts":   job.Attempts,
			}
			result, err := filterExpr.Evaluate(matchCtx)
			if err != nil {
				m.log.Debug().Err(err).Msg("Filter evaluation failed")
				continue
			}
			if b, ok := result.(bool); !ok || !b {
				continue
			}
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// Receive receives one or more jobs from the queue
func (m *Manager) Receive(ctx context.Context, resourcePath string, maxJobs int, options ReserveOptions) ([]*JobMetadata, error) {
	// Check context first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if maxJobs <= 0 {
		maxJobs = 1
	}
	if maxJobs > 100 {
		maxJobs = 100 // Limit batch size
	}

	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return nil, err
	}

	// Use default options if not provided
	if options.VisibilityTimeout == 0 {
		options = DefaultReserveOptions()
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	var jobs []*JobMetadata
	now := time.Now()

	state.mu.Lock()
	for len(jobs) < maxJobs {
		job := state.ReadyHeap.Peek()
		if job == nil {
			break
		}

		// Check if job is visible
		if job.VisibleAt.After(now) {
			break
		}

		// Pop the job
		popped := state.ReadyHeap.PopJob()
		if popped == nil {
			break
		}

		// Add to InFlight
		popped.ReserveUntil = now.Add(options.VisibilityTimeout)
		popped.Attempts++
		state.InFlight[popped.ID] = popped
		jobs = append(jobs, popped)
	}
	state.mu.Unlock()

	// If no jobs and long polling enabled, wait
	if len(jobs) == 0 && options.LongPollTimeout > 0 {
		job, err := m.longPollForJob(ctx, resourcePath, options.LongPollTimeout, options.MaxWaitTime)
		if err != nil {
			return nil, err
		}
		if job != nil {
			// Add to InFlight
			inFlightJob, err := m.AddToInFlight(resourcePath, job.ID, options.VisibilityTimeout)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, inFlightJob)
		}
	}

	return jobs, nil
}

// ReceiveWithContext receives jobs with context support
func (m *Manager) ReceiveWithContext(ctx context.Context, resourcePath string, maxJobs int, options ReserveOptions) ([]*JobMetadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return m.Receive(ctx, resourcePath, maxJobs, options)
}

// RegisterWorker registers a worker for a queue
func (m *Manager) RegisterWorker(ctx context.Context, resourcePath string, workerID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return QueueNotFoundError{ResourcePath: resourcePath}
		}
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize workers map for this queue if needed
	if m.workers[resourcePath] == nil {
		m.workers[resourcePath] = make(map[string]*WorkerInfo)
	}

	// Create or update worker info
	worker := &WorkerInfo{
		WorkerID:      workerID,
		QueuePath:     resourcePath,
		LastHeartbeat: time.Now(),
		ActiveJobs:    make([]string, 0),
	}
	m.workers[resourcePath][workerID] = worker

	m.log.Debug().
		Str("resource", resourcePath).
		Str("worker_id", workerID).
		Msg("Worker registered")

	return nil
}

// UnregisterWorker unregisters a worker
func (m *Manager) UnregisterWorker(ctx context.Context, resourcePath string, workerID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	workers, exists := m.workers[resourcePath]
	if !exists {
		return WorkerNotFoundError{WorkerID: workerID, ResourcePath: resourcePath}
	}

	worker, exists := workers[workerID]
	if !exists {
		return WorkerNotFoundError{WorkerID: workerID, ResourcePath: resourcePath}
	}

	// Requeue any active jobs
	for _, jobID := range worker.ActiveJobs {
		// Get job from InFlight
		state, stateExists := m.queueStates[resourcePath]
		if stateExists {
			state.mu.Lock()
			if job, jobExists := state.InFlight[jobID]; jobExists {
				// Requeue the job
				job.VisibleAt = time.Now()
				job.ReserveUntil = time.Time{}
				state.ReadyHeap.PushJob(job)
				delete(state.InFlight, jobID)
			}
			state.mu.Unlock()
		}
	}

	delete(workers, workerID)
	if len(workers) == 0 {
		delete(m.workers, resourcePath)
	}

	m.log.Debug().
		Str("resource", resourcePath).
		Str("worker_id", workerID).
		Msg("Worker unregistered")

	return nil
}

// Heartbeat updates worker heartbeat
func (m *Manager) Heartbeat(ctx context.Context, resourcePath string, workerID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	workers, exists := m.workers[resourcePath]
	if !exists {
		return WorkerNotFoundError{WorkerID: workerID, ResourcePath: resourcePath}
	}

	worker, exists := workers[workerID]
	if !exists {
		return WorkerNotFoundError{WorkerID: workerID, ResourcePath: resourcePath}
	}

	worker.LastHeartbeat = time.Now()
	return nil
}

// GetJobPayload retrieves the payload for a job from the log
func (m *Manager) GetJobPayload(ctx context.Context, resourcePath string, jobID string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Get job metadata
	job, err := m.GetJobMetadata(ctx, resourcePath, jobID)
	if err != nil {
		return nil, err
	}

	// Read from segment
	reader, err := log.NewSegmentReader(job.PayloadPos.File)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer reader.Close()

	// Seek to the offset
	// Note: We need to read entries until we find the one at the correct offset
	// For now, we'll read from the beginning and find the matching entry
	for {
		data, offset, readErr := reader.ReadEntry()
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read entry: %w", readErr)
		}

		// Check if this is the entry we're looking for
		if offset == job.PayloadPos.Offset {
			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				return nil, fmt.Errorf("failed to decode message: %w", decodeErr)
			}

			// Verify job ID matches
			if msg.ID != jobID {
				return nil, fmt.Errorf("job ID mismatch: expected %s, got %s", jobID, msg.ID)
			}

			return msg.Payload, nil
		}

		// If we've passed the offset, the entry wasn't found
		if offset > job.PayloadPos.Offset {
			return nil, fmt.Errorf("job entry not found at offset %d", job.PayloadPos.Offset)
		}
	}

	return nil, fmt.Errorf("job entry not found")
}

// GetJobMetadata retrieves job metadata by ID
func (m *Manager) GetJobMetadata(ctx context.Context, resourcePath string, jobID string) (*JobMetadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	// Check InFlight first
	if job, exists := state.InFlight[jobID]; exists {
		return job, nil
	}

	// Check ReadyHeap
	allJobs := state.ReadyHeap.GetAll()
	for _, job := range allJobs {
		if job.ID == jobID {
			return job, nil
		}
	}

	return nil, JobNotFoundError{JobID: jobID, ResourcePath: resourcePath}
}

// GetQueueStats returns statistics for a queue
func (m *Manager) GetQueueStats(ctx context.Context, resourcePath string) (*QueueStats, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Validate queue exists
	_, err := m.metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, QueueNotFoundError{ResourcePath: resourcePath}
		}
		return nil, err
	}

	// Get queue state
	m.mu.RLock()
	state, exists := m.queueStates[resourcePath]
	m.mu.RUnlock()

	if !exists {
		return nil, QueueNotFoundError{ResourcePath: resourcePath}
	}

	state.mu.RLock()
	defer state.mu.RUnlock()

	now := time.Now()
	stats := &QueueStats{
		TotalJobs:     state.NextSeq,
		PendingJobs:   int64(state.ReadyHeap.Len()),
		InFlightJobs:  int64(len(state.InFlight)),
		CompletedJobs: 0, // Would need to track this separately
		FailedJobs:    0, // Would need to track this separately
		OldestJobAge:  0,
	}

	// Find oldest job
	allJobs := state.ReadyHeap.GetAll()
	if len(allJobs) > 0 {
		oldest := allJobs[0]
		for _, job := range allJobs {
			if job.CreatedAt.Before(oldest.CreatedAt) {
				oldest = job
			}
		}
		stats.OldestJobAge = now.Sub(oldest.CreatedAt)
	}

	// Update metrics
	if m.metrics != nil {
		if config, configErr := m.metaStore.GetResource(resourcePath); configErr == nil {
			m.metrics.UpdateQueueStats(config.Tenant, config.Namespace, config.Name, stats.PendingJobs, stats.InFlightJobs, stats.OldestJobAge.Seconds())
		}
	}

	return stats, nil
}

// recoverLastSeq recovers the last sequence number by reading the last segment
func (m *Manager) recoverLastSeq(resourcePath string, partition int32) (int64, error) {
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return -1, err
	}

	if len(segments) == 0 {
		return -1, nil // No messages
	}

	// Read last segment to find last sequence
	lastSegment := segments[len(segments)-1]
	reader, err := log.NewSegmentReader(lastSegment.Path)
	if err != nil {
		return -1, err
	}
	defer reader.Close()

	var lastSeq int64 = -1
	for {
		data, _, err := reader.ReadEntry()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return -1, err
		}

		msg, err := log.DecodeMessage(data)
		if err != nil {
			continue // Skip corrupted messages
		}

		if msg.Partition == partition && msg.Type == log.MessageTypeQueue && msg.Seq > lastSeq {
			lastSeq = msg.Seq
		}
	}

	return lastSeq, nil
}

// rebuildReadyHeap rebuilds the ReadyHeap from log entries
func (m *Manager) rebuildReadyHeap(resourcePath string, partition int32, state *QueueState) error {
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		return err
	}

	now := time.Now()

	// Read all segments and rebuild heap
	for _, seg := range segments {
		reader, err := log.NewSegmentReader(seg.Path)
		if err != nil {
			continue // Skip segments that can't be read
		}

		for {
			data, offset, readErr := reader.ReadEntry()
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					break
				}
				// Skip corrupted entries, continue reading
				continue
			}

			// Decode message
			msg, decodeErr := log.DecodeMessage(data)
			if decodeErr != nil {
				// Skip corrupted messages, continue reading
				continue
			}

			// Only process queue messages for this partition
			if msg.Partition != partition || msg.Type != log.MessageTypeQueue {
				continue
			}

			// Check if job is already completed (not in InFlight, so assume ready if visible)
			// For MVP, we assume all jobs in log are ready (InFlight is in-memory only)
			if msg.VisibleAt.Before(now) || msg.VisibleAt.Equal(now) {
				// Create job metadata
				jobMetadata := &JobMetadata{
					ID:           msg.ID,
					Seq:          msg.Seq,
					VisibleAt:    msg.VisibleAt,
					ReserveUntil: time.Time{},
					PayloadPos: log.FilePointer{
						File:   seg.Path,
						Offset: offset,
					},
					Attempts:  msg.Attempts,
					CreatedAt: msg.CreatedAt,
				}

				// Add to ReadyHeap
				state.ReadyHeap.PushJob(jobMetadata)
			}
		}

		reader.Close()
	}

	return nil
}
