package auth

import (
	"fmt"
)

// Authorizer defines the interface for authorization
type Authorizer interface {
	// Authorize checks if the auth context is authorized for the given operation
	Authorize(ctx *AuthContext, tenant, namespace string, permission Permission) error
}

// PermissionAuthorizer implements authorization logic
type PermissionAuthorizer struct {
}

// NewPermissionAuthorizer creates a new permission authorizer
func NewPermissionAuthorizer() *PermissionAuthorizer {
	return &PermissionAuthorizer{}
}

// Authorize checks if the auth context is authorized for the given operation
func (a *PermissionAuthorizer) Authorize(ctx *AuthContext, tenant, namespace string, permission Permission) error {
	if ctx == nil {
		return UnauthorizedError{Reason: "no auth context"}
	}

	// Check tenant matches
	if ctx.Tenant != tenant {
		return ForbiddenError{
			Resource: fmt.Sprintf("%s/%s", tenant, namespace),
			Action:   string(permission),
			Reason:   fmt.Sprintf("tenant mismatch: token tenant '%s' does not match resource tenant '%s'", ctx.Tenant, tenant),
		}
	}

	// Check namespace is allowed
	if !ctx.IsNamespaceAllowed(namespace) {
		return ForbiddenError{
			Resource: fmt.Sprintf("%s/%s", tenant, namespace),
			Action:   string(permission),
			Reason:   fmt.Sprintf("namespace '%s' is not allowed for this token", namespace),
		}
	}

	// Check permission
	if !ctx.HasPermission(permission) {
		return ForbiddenError{
			Resource: fmt.Sprintf("%s/%s", tenant, namespace),
			Action:   string(permission),
			Reason:   "token does not have required permission",
		}
	}

	return nil
}
