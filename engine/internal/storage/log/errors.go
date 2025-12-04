package log

import "fmt"

// EntryTooLargeError indicates an entry exceeds the maximum size
type EntryTooLargeError struct {
	Size int
	Max  int
}

func (e EntryTooLargeError) Error() string {
	return fmt.Sprintf("entry size %d exceeds maximum %d", e.Size, e.Max)
}

// InvalidEntryLengthError indicates an invalid entry length
type InvalidEntryLengthError struct {
	Length uint32
}

func (e InvalidEntryLengthError) Error() string {
	return fmt.Sprintf("invalid entry length: %d", e.Length)
}

// ChecksumMismatchError indicates a checksum validation failure
type ChecksumMismatchError struct {
	Expected uint32
	Actual   uint32
}

func (e ChecksumMismatchError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %d, got %d", e.Expected, e.Actual)
}

// SegmentNotFoundError indicates a segment file was not found
type SegmentNotFoundError struct {
	Path string
}

func (e SegmentNotFoundError) Error() string {
	return fmt.Sprintf("segment not found: %s", e.Path)
}

// SegmentClosedError indicates a segment operation on a closed segment
type SegmentClosedError struct {
	Path string
}

func (e SegmentClosedError) Error() string {
	return fmt.Sprintf("segment is closed: %s", e.Path)
}
