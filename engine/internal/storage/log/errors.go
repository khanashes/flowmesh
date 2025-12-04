package log

import "fmt"

// ErrEntryTooLarge indicates an entry exceeds the maximum size
type ErrEntryTooLarge struct {
	Size int
	Max  int
}

func (e ErrEntryTooLarge) Error() string {
	return fmt.Sprintf("entry size %d exceeds maximum %d", e.Size, e.Max)
}

// ErrInvalidEntryLength indicates an invalid entry length
type ErrInvalidEntryLength struct {
	Length uint32
}

func (e ErrInvalidEntryLength) Error() string {
	return fmt.Sprintf("invalid entry length: %d", e.Length)
}

// ErrChecksumMismatch indicates a checksum validation failure
type ErrChecksumMismatch struct {
	Expected uint32
	Actual   uint32
}

func (e ErrChecksumMismatch) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %d, got %d", e.Expected, e.Actual)
}

// ErrSegmentNotFound indicates a segment file was not found
type ErrSegmentNotFound struct {
	Path string
}

func (e ErrSegmentNotFound) Error() string {
	return fmt.Sprintf("segment not found: %s", e.Path)
}

// ErrSegmentClosed indicates a segment operation on a closed segment
type ErrSegmentClosed struct {
	Path string
}

func (e ErrSegmentClosed) Error() string {
	return fmt.Sprintf("segment is closed: %s", e.Path)
}
