package client

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error represents an SDK error
type Error struct {
	Code    codes.Code
	Message string
	Details []interface{}
	Err     error
}

func (e *Error) Error() string {
	if len(e.Details) > 0 {
		return fmt.Sprintf("%s: %s (%v)", e.Code, e.Message, e.Details)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error {
	return e.Err
}

// IsNotFound returns true if the error is a not found error
func (e *Error) IsNotFound() bool {
	return e.Code == codes.NotFound
}

// IsUnauthenticated returns true if the error is an authentication error
func (e *Error) IsUnauthenticated() bool {
	return e.Code == codes.Unauthenticated
}

// IsPermissionDenied returns true if the error is a permission denied error
func (e *Error) IsPermissionDenied() bool {
	return e.Code == codes.PermissionDenied
}

// wrapError wraps a gRPC error into an SDK Error
func wrapError(err error, operation string) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return &Error{
			Code:    codes.Unknown,
			Message: fmt.Sprintf("%s failed", operation),
			Err:     err,
		}
	}

	return &Error{
		Code:    st.Code(),
		Message: fmt.Sprintf("%s failed: %s", operation, st.Message()),
		Details: st.Details(),
		Err:     err,
	}
}
