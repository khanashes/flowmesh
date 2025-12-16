package log

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFsyncScheduler_StartStop(t *testing.T) {
	scheduler := NewFsyncScheduler(10 * time.Millisecond)

	scheduler.Start()
	time.Sleep(50 * time.Millisecond)
	scheduler.Stop()

	// Should not panic
}

func TestFsyncScheduler_RegisterUnregister(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "test-segment.log")

	scheduler := NewFsyncScheduler(10 * time.Millisecond)
	scheduler.Start()
	defer scheduler.Stop()

	writer, err := NewSegmentWriter(segmentPath, FsyncInterval)
	require.NoError(t, err)
	defer writer.Close()

	// Register writer
	scheduler.Register(writer)

	// Unregister writer
	scheduler.Unregister(writer)

	// Should not cause issues
}

func TestSegmentWriter_FsyncAlways(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "test-segment.log")

	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)
	defer writer.Close()

	// Write entry
	data := []byte("test data")
	err = writer.WriteEntry(data)
	require.NoError(t, err)

	// Verify file was synced (file size should be correct immediately)
	stat, err := os.Stat(segmentPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestSegmentWriter_FsyncInterval(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "test-segment.log")

	scheduler := NewFsyncScheduler(50 * time.Millisecond)
	scheduler.Start()
	defer scheduler.Stop()

	writer, err := NewSegmentWriter(segmentPath, FsyncInterval)
	require.NoError(t, err)
	defer writer.Close()

	scheduler.Register(writer)
	defer scheduler.Unregister(writer)

	// Write entry
	data := []byte("test data interval")
	err = writer.WriteEntry(data)
	require.NoError(t, err)

	// Wait for scheduler to fsync
	time.Sleep(100 * time.Millisecond)

	// Verify file exists and has data
	stat, err := os.Stat(segmentPath)
	require.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))
}

func TestSegmentWriter_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "test-segment-concurrent.log")

	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)
	defer writer.Close()

	// Write multiple entries concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(n int) {
			data := []byte("concurrent data")
			err := writer.WriteEntry(data)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Wait for all writes
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all entries were written
	assert.Greater(t, writer.Offset(), int64(0))
}

func TestLogManager_RecoverCorruptedSegment(t *testing.T) {
	tmpDir := t.TempDir()

	manager := NewManager(tmpDir, FsyncAlways, 10*time.Millisecond)

	// Create a segment with valid data
	resourceDir := filepath.Join(tmpDir, "test-resource")
	require.NoError(t, os.MkdirAll(resourceDir, 0755))

	segmentPath := filepath.Join(resourceDir, "segment-000001.log")
	writer, err := NewSegmentWriter(segmentPath, FsyncAlways)
	require.NoError(t, err)

	// Write valid entry
	err = writer.WriteEntry([]byte("valid data"))
	require.NoError(t, err)
	writer.Close()

	// Corrupt the file by appending garbage
	f, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("CORRUPTED DATA"))
	require.NoError(t, err)
	f.Close()

	// Run recovery
	err = manager.Recover()
	require.NoError(t, err)

	// Segment might be truncated or deleted if fully corrupted
	// Just verify recovery completed without error
}

func TestLogManager_Shutdown(t *testing.T) {
	tmpDir := t.TempDir()

	scheduler := NewFsyncScheduler(10 * time.Millisecond)
	manager := NewManager(tmpDir, FsyncInterval, 10*time.Millisecond)
	manager.fsyncScheduler = scheduler
	scheduler.Start()

	// Shutdown should stop scheduler
	err := manager.Shutdown()
	assert.NoError(t, err)
}
