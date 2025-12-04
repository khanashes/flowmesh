package metastore

import "fmt"

// ErrResourceNotFound indicates a resource was not found
type ErrResourceNotFound struct {
	Path string
}

func (e ErrResourceNotFound) Error() string {
	return fmt.Sprintf("resource not found: %s", e.Path)
}

// ErrResourceExists indicates a resource already exists
type ErrResourceExists struct {
	Path string
}

func (e ErrResourceExists) Error() string {
	return fmt.Sprintf("resource already exists: %s", e.Path)
}

// ErrInvalidConfig indicates an invalid resource configuration
type ErrInvalidConfig struct {
	Field  string
	Reason string
}

func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid config field '%s': %s", e.Field, e.Reason)
}
