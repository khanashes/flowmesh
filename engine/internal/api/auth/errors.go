package auth

import "fmt"

// UnauthorizedError indicates the request is not authenticated
type UnauthorizedError struct {
	Reason string
}

func (e UnauthorizedError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("unauthorized: %s", e.Reason)
	}
	return "unauthorized"
}

// ForbiddenError indicates the request is authenticated but not authorized
type ForbiddenError struct {
	Resource string
	Action   string
	Reason   string
}

func (e ForbiddenError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("forbidden: %s on %s: %s", e.Action, e.Resource, e.Reason)
	}
	return fmt.Sprintf("forbidden: %s on %s", e.Action, e.Resource)
}

// TokenNotFoundError indicates the token was not found
type TokenNotFoundError struct {
	TokenHash string
}

func (e TokenNotFoundError) Error() string {
	return fmt.Sprintf("token not found: %s", e.TokenHash)
}
