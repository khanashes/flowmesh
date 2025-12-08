package queues

import (
	"context"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog"
)

// Scheduler manages visibility timeout expiry and job requeuing
type Scheduler struct {
	metaStore  *metastore.Store
	logManager *log.Manager
	queueMgr   *Manager
	intervals  map[string]time.Duration // queuePath -> interval
	stopChans  map[string]chan struct{} // queuePath -> stop channel
	log        zerolog.Logger
	mu         sync.RWMutex
	running    bool
}

// NewScheduler creates a new scheduler
func NewScheduler(metaStore *metastore.Store, logManager *log.Manager) *Scheduler {
	return &Scheduler{
		metaStore:  metaStore,
		logManager: logManager,
		intervals:  make(map[string]time.Duration),
		stopChans:  make(map[string]chan struct{}),
		log:        logger.WithComponent("queues.scheduler"),
		running:    false,
	}
}

// SetQueueManager sets the queue manager (needed for requeuing)
func (s *Scheduler) SetQueueManager(queueMgr *Manager) {
	s.queueMgr = queueMgr
}

// Start starts the scheduler for all queues
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	s.running = true
	s.log.Info().Msg("Scheduler started")

	// Start background goroutine to process all queues
	go s.run(ctx)

	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false

	// Stop all queue schedulers
	for queuePath, stopChan := range s.stopChans {
		close(stopChan)
		delete(s.stopChans, queuePath)
		s.log.Debug().Str("queue", queuePath).Msg("Stopped scheduler for queue")
	}

	s.log.Info().Msg("Scheduler stopped")
	return nil
}

// run is the main scheduler loop
func (s *Scheduler) run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.processAllQueues()
		}
	}
}

// processAllQueues processes expired jobs for all queues
func (s *Scheduler) processAllQueues() {
	if s.queueMgr == nil {
		return
	}

	// Get all queue states
	s.queueMgr.mu.RLock()
	queueStates := make(map[string]*QueueState)
	for path, state := range s.queueMgr.queueStates {
		queueStates[path] = state
	}
	s.queueMgr.mu.RUnlock()

	// Process each queue
	for queuePath, state := range queueStates {
		s.processExpiredJobs(queuePath, state)
	}
}

// processExpiredJobs processes expired jobs for a specific queue
func (s *Scheduler) processExpiredJobs(resourcePath string, state *QueueState) {
	now := time.Now()
	var expiredJobs []*JobMetadata

	state.mu.Lock()
	// Find expired jobs
	for jobID, job := range state.InFlight {
		if !job.ReserveUntil.IsZero() && job.ReserveUntil.Before(now) {
			expiredJobs = append(expiredJobs, job)
			delete(state.InFlight, jobID)
		}
	}
	state.mu.Unlock()

	// Process expired jobs
	for _, job := range expiredJobs {
		if err := s.handleExpiredJob(resourcePath, job); err != nil {
			s.log.Error().
				Err(err).
				Str("queue", resourcePath).
				Str("job_id", job.ID).
				Msg("Failed to handle expired job")
		}
	}
}

// handleExpiredJob handles a single expired job
func (s *Scheduler) handleExpiredJob(resourcePath string, job *JobMetadata) error {
	// Get retry policy
	s.queueMgr.mu.RLock()
	policy := s.queueMgr.retryPolicies[resourcePath]
	s.queueMgr.mu.RUnlock()

	// Use default policy if not set
	if policy == nil {
		defaultPolicy := DefaultRetryPolicy()
		policy = &defaultPolicy
	}

	// Check max attempts
	if policy.MaxAttempts > 0 && job.Attempts >= policy.MaxAttempts {
		// Max attempts exceeded - will be moved to DLQ in Phase 12
		s.log.Warn().
			Str("queue", resourcePath).
			Str("job_id", job.ID).
			Int32("attempts", job.Attempts).
			Msg("Job exceeded max attempts, will be moved to DLQ")
		return nil
	}

	// Calculate backoff
	delay := s.queueMgr.calculateBackoff(job.Attempts, policy)

	// Requeue job
	return s.queueMgr.requeueJob(resourcePath, job, delay)
}
