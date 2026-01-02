package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// EntryHeaderSize is the size of entry header (length + checksum)
	EntryHeaderSize = 8 // 4 bytes length + 4 bytes checksum
	// MaxEntrySize is the maximum entry size (10MB)
	MaxEntrySize = 10 * 1024 * 1024
)

var (
	// CRC32Table for checksum calculation
	CRC32Table = crc32.MakeTable(crc32.IEEE)
)

// SegmentWriter handles writing entries to a segment file
type SegmentWriter struct {
	file          *os.File
	path          string
	offset        int64
	fsyncPolicy   FsyncPolicy
	lastFsyncTime time.Time
	mu            sync.Mutex
}

// NewSegmentWriter creates a new segment writer
func NewSegmentWriter(path string, policy FsyncPolicy) (*SegmentWriter, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Open file for append/create
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		//nolint:errcheck // Ignore close error
		_ = file.Close()
		return nil, err
	}

	writer := &SegmentWriter{
		file:          file,
		path:          path,
		offset:        stat.Size(),
		fsyncPolicy:   policy,
		lastFsyncTime: time.Now(),
	}

	return writer, nil
}

// WriteEntry writes an entry to the segment file
// Format: [EntryLength (4 bytes)][EntryBytes (variable)][Checksum (4 bytes)]
func (sw *SegmentWriter) WriteEntry(data []byte) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if len(data) == 0 {
		return nil
	}

	if len(data) > MaxEntrySize {
		return EntryTooLargeError{Size: len(data), Max: MaxEntrySize}
	}

	// Calculate checksum of data
	checksum := crc32.Checksum(data, CRC32Table)

	// Write length (4 bytes)
	// Guard against integer overflow
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("entry too large: %d bytes exceeds maximum %d", len(data), math.MaxUint32) //nolint:goerr113
	}
	length := uint32(len(data))
	if err := binary.Write(sw.file, binary.BigEndian, length); err != nil {
		return err
	}

	// Write data
	if _, err := sw.file.Write(data); err != nil {
		return err
	}

	// Write checksum (4 bytes)
	if err := binary.Write(sw.file, binary.BigEndian, checksum); err != nil {
		return err
	}

	// Update offset
	sw.offset += EntryHeaderSize + int64(len(data))

	// Fsync based on policy
	if sw.fsyncPolicy == FsyncAlways {
		if err := sw.file.Sync(); err != nil {
			return err
		}
		sw.lastFsyncTime = time.Now()
	}
	// For FsyncInterval, syncing is handled by FsyncScheduler

	return nil
}

// Flush flushes any buffered data to disk
func (sw *SegmentWriter) Flush() error {
	if sw.file != nil {
		return sw.file.Sync()
	}
	return nil
}

// Close closes the segment writer
func (sw *SegmentWriter) Close() error {
	if sw.file != nil {
		return sw.file.Close()
	}
	return nil
}

// Offset returns the current write offset
func (sw *SegmentWriter) Offset() int64 {
	return sw.offset
}

// SegmentReader handles reading entries from a segment file
type SegmentReader struct {
	file   *os.File
	path   string
	offset int64
}

// NewSegmentReader creates a new segment reader
func NewSegmentReader(path string) (*SegmentReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	reader := &SegmentReader{
		file:   file,
		path:   path,
		offset: 0,
	}

	return reader, nil
}

// ReadEntry reads the next entry from the segment file
func (sr *SegmentReader) ReadEntry() (data []byte, offset int64, err error) {
	// Read length (4 bytes)
	var length uint32
	if err := binary.Read(sr.file, binary.BigEndian, &length); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, io.EOF
		}
		return nil, 0, err
	}

	if length == 0 || length > MaxEntrySize {
		return nil, 0, InvalidEntryLengthError{Length: length}
	}

	// Remember offset before reading data
	offset = sr.offset

	// Read data
	data = make([]byte, length)
	if _, err = io.ReadFull(sr.file, data); err != nil {
		return nil, 0, err
	}

	// Read checksum (4 bytes)
	var storedChecksum uint32
	if err = binary.Read(sr.file, binary.BigEndian, &storedChecksum); err != nil {
		return nil, 0, err
	}

	// Verify checksum
	calculatedChecksum := crc32.Checksum(data, CRC32Table)
	if calculatedChecksum != storedChecksum {
		return nil, 0, ChecksumMismatchError{
			Expected: storedChecksum,
			Actual:   calculatedChecksum,
		}
	}

	// Update offset
	sr.offset += EntryHeaderSize + int64(length)

	return data, offset, nil
}

// Seek seeks to a specific offset in the file
func (sr *SegmentReader) Seek(offset int64, whence int) (int64, error) {
	newOffset, err := sr.file.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	sr.offset = newOffset
	return newOffset, nil
}

// Close closes the segment reader
func (sr *SegmentReader) Close() error {
	if sr.file != nil {
		return sr.file.Close()
	}
	return nil
}

// ValidateSegment validates a segment file for corruption
func ValidateSegment(path string) error {
	reader, err := NewSegmentReader(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			// Ignore close errors in defer
		}
	}()

	// Read through all entries to check for corruption
	for {
		_, _, err := reader.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}
