package queues

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/flowmesh/engine/internal/storage/log"
	"github.com/flowmesh/engine/internal/storage/metastore"
)

// Recover recovers the state of all queues from the log
func (m *Manager) Recover(ctx context.Context) error {
	m.log.Info().Msg("Starting queue recovery")

	// List all queue resources
	queues, err := m.metaStore.ListResources("", "", metastore.ResourceQueue)
	if err != nil {
		return fmt.Errorf("failed to list queues: %w", err)
	}

	m.log.Info().Int("count", len(queues)).Msg("Found queues to recover")

	for _, config := range queues {
		path := config.GetPath()
		if err := m.RecoverQueue(ctx, path); err != nil {
			return fmt.Errorf("failed to recover queue %s: %w", path, err)
		}
	}

	m.log.Info().Msg("Queue recovery completed")
	return nil
}

// RecoverQueue recovers a single queue state
func (m *Manager) RecoverQueue(ctx context.Context, resourcePath string) error {
	m.log.Debug().Str("queue", resourcePath).Msg("Recovering queue")

	// Get or create empty state
	state := m.getOrCreateQueueState(resourcePath)
	state.mu.Lock()
	defer state.mu.Unlock()

	// Reset state
	state.NextSeq = 0
	state.ReadyHeap = NewJobMinHeap()
	state.InFlight = make(map[string]*JobMetadata)

	// Map to track jobs found in log (ID -> JobMetadata)
	knownJobs := make(map[string]*JobMetadata)

	// List all segments for partition 0 (MVP)
	partition := int32(0)
	segments, err := m.logManager.ListSegments(resourcePath, partition)
	if err != nil {
		// No segments found is not an error (new queue)
		m.log.Debug().Str("queue", resourcePath).Msg("No segments found, skipping recovery")
		return nil
	}

	// Iterate segments and replay
	for _, segment := range segments {
		reader, err := log.NewSegmentReader(segment.Path)
		if err != nil {
			return fmt.Errorf("failed to create segment reader: %w", err)
		}
		defer reader.Close() //nolint:gocritic // Defer inside loop is fine for file handles here

		for {
			data, offset, err := reader.ReadEntry()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("failed to read entry: %w", err)
			}

			msg, err := log.DecodeMessage(data)
			if err != nil {
				// Stop recovery on corruption?
				// For strict correctness, yes.
				return fmt.Errorf("failed to decode message at offset %d: %w", offset, err)
			}

			switch msg.Type {
			case log.MessageTypeQueue:
				// New job or re-enqueue
				job := &JobMetadata{
					ID:           msg.ID,
					Seq:          msg.Seq,
					VisibleAt:    msg.VisibleAt,
					ReserveUntil: time.Time{}, // Reset on recovery
					PayloadPos: log.FilePointer{
						File:   segment.Path,
						Offset: offset,
					},
					Attempts:  msg.Attempts,
					CreatedAt: msg.CreatedAt,
				}
				knownJobs[msg.ID] = job

				// Track NextSeq
				if msg.Seq >= state.NextSeq {
					state.NextSeq = msg.Seq + 1
				}

			case log.MessageTypeQueueAck:
				// Job completed
				delete(knownJobs, msg.ID)
			}
		}
	}

	// Add un-ACKed jobs to ReadyHeap
	count := 0
	for _, job := range knownJobs {
		state.ReadyHeap.PushJob(job)
		count++
	}

	m.log.Info().
		Str("queue", resourcePath).
		Int("recovered_jobs", count).
		Int64("next_seq", state.NextSeq).
		Msg("Queue recovered")

	return nil
}
