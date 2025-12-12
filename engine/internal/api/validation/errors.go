package validation

import "fmt"

// ValidationError indicates a validation error
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s", e.Field, e.Reason)
}
