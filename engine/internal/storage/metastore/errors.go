package metastore

import "fmt"

// ResourceNotFoundError indicates a resource was not found
type ResourceNotFoundError struct {
	Path string
}

func (e ResourceNotFoundError) Error() string {
	return fmt.Sprintf("resource not found: %s", e.Path)
}

// ResourceExistsError indicates a resource already exists
type ResourceExistsError struct {
	Path string
}

func (e ResourceExistsError) Error() string {
	return fmt.Sprintf("resource already exists: %s", e.Path)
}

// InvalidConfigError indicates an invalid resource configuration
type InvalidConfigError struct {
	Field  string
	Reason string
}

func (e InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid config field '%s': %s", e.Field, e.Reason)
}
