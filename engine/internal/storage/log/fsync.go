package log

import (
	"sync"
	"time"
)

// FsyncPolicy defines when to sync data to disk
type FsyncPolicy string

const (
	// FsyncAlways syncs after every write (maximum durability, lower throughput)
	FsyncAlways FsyncPolicy = "always"
	// FsyncInterval syncs at regular time intervals (optimal for most use cases)
	FsyncInterval FsyncPolicy = "interval"
)

// FsyncScheduler manages periodic fsyncing for interval-based policy
type FsyncScheduler struct {
	interval time.Duration
	writers  map[*SegmentWriter]struct{}
	mu       sync.RWMutex
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewFsyncScheduler creates a new fsync scheduler
func NewFsyncScheduler(interval time.Duration) *FsyncScheduler {
	return &FsyncScheduler{
		interval: interval,
		writers:  make(map[*SegmentWriter]struct{}),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the fsync scheduler
func (fs *FsyncScheduler) Start() {
	fs.wg.Add(1)
	go fs.run()
}

// Stop stops the fsync scheduler
func (fs *FsyncScheduler) Stop() {
	close(fs.stopCh)
	fs.wg.Wait()
}

// Register registers a segment writer for periodic fsyncing
func (fs *FsyncScheduler) Register(sw *SegmentWriter) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.writers[sw] = struct{}{}
}

// Unregister removes a segment writer from periodic fsyncing
func (fs *FsyncScheduler) Unregister(sw *SegmentWriter) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.writers, sw)
}

// run executes the fsync loop
func (fs *FsyncScheduler) run() {
	defer fs.wg.Done()

	ticker := time.NewTicker(fs.interval)
	defer ticker.Stop()

	for {
		select {
		case <-fs.stopCh:
			// Final fsync before shutdown
			fs.flushAll()
			return
		case <-ticker.C:
			fs.flushAll()
		}
	}
}

// flushAll flushes all registered writers
func (fs *FsyncScheduler) flushAll() {
	fs.mu.RLock()
	writers := make([]*SegmentWriter, 0, len(fs.writers))
	for w := range fs.writers {
		writers = append(writers, w)
	}
	fs.mu.RUnlock()

	for _, w := range writers {
		//nolint:errcheck // Ignore errors, individual writers log them
		_ = w.Flush()
	}
}
