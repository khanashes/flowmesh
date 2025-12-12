package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPermissionAuthorizer_EdgeCases(t *testing.T) {
	authorizer := NewPermissionAuthorizer()

	t.Run("Authorize with nil context", func(t *testing.T) {
		err := authorizer.Authorize(nil, "tenant", "ns", PermissionKVRead)
		assert.Error(t, err)
		assert.IsType(t, UnauthorizedError{}, err)
	})

	t.Run("Authorize with tenant mismatch", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant1",
			AllowedNS:   []string{"ns1"},
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "tenant2", "ns1", PermissionKVRead)
		assert.Error(t, err)
		assert.IsType(t, ForbiddenError{}, err)
	})

	t.Run("Authorize with namespace not allowed", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant1",
			AllowedNS:   []string{"ns1"}, // Only ns1 allowed
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "tenant1", "ns2", PermissionKVRead)
		assert.Error(t, err)
		assert.IsType(t, ForbiddenError{}, err)
	})

	t.Run("Authorize with missing permission", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant1",
			AllowedNS:   []string{"ns1"},
			Permissions: []Permission{PermissionKVRead}, // Only read, no write
		}

		err := authorizer.Authorize(ctx, "tenant1", "ns1", PermissionKVWrite)
		assert.Error(t, err)
		assert.IsType(t, ForbiddenError{}, err)
	})

	t.Run("Authorize with empty AllowedNS (all namespaces allowed)", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant1",
			AllowedNS:   []string{}, // Empty means all allowed
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "tenant1", "any-namespace", PermissionKVRead)
		assert.NoError(t, err)
	})

	t.Run("Authorize with nil AllowedNS (all namespaces allowed)", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant1",
			AllowedNS:   nil, // Nil means all allowed
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "tenant1", "any-namespace", PermissionKVRead)
		assert.NoError(t, err)
	})

	t.Run("Authorize with unicode tenant and namespace", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "租户",
			AllowedNS:   []string{"命名空间"},
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "租户", "命名空间", PermissionKVRead)
		assert.NoError(t, err)

		// Wrong namespace
		err = authorizer.Authorize(ctx, "租户", "其他命名空间", PermissionKVRead)
		assert.Error(t, err)
	})

	t.Run("Authorize with empty tenant", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "",
			AllowedNS:   []string{},
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "", "ns", PermissionKVRead)
		assert.NoError(t, err) // Empty tenant matches empty tenant
	})

	t.Run("Authorize with empty namespace", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant",
			AllowedNS:   []string{""}, // Empty namespace explicitly allowed
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, "tenant", "", PermissionKVRead)
		assert.NoError(t, err)
	})

	t.Run("Authorize with all permissions", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:    "tenant",
			AllowedNS: []string{"ns"},
			Permissions: []Permission{
				PermissionKVRead,
				PermissionKVWrite,
				PermissionQueueRead,
				PermissionQueueWrite,
				PermissionStreamRead,
				PermissionStreamWrite,
			},
		}

		allPerms := []Permission{
			PermissionKVRead,
			PermissionKVWrite,
			PermissionQueueRead,
			PermissionQueueWrite,
			PermissionStreamRead,
			PermissionStreamWrite,
		}

		for _, perm := range allPerms {
			err := authorizer.Authorize(ctx, "tenant", "ns", perm)
			assert.NoError(t, err, "should authorize %s", perm)
		}
	})

	t.Run("Authorize with no permissions", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "tenant",
			AllowedNS:   []string{"ns"},
			Permissions: []Permission{}, // No permissions
		}

		err := authorizer.Authorize(ctx, "tenant", "ns", PermissionKVRead)
		assert.Error(t, err)
		assert.IsType(t, ForbiddenError{}, err)
	})

	t.Run("Authorize case sensitivity", func(t *testing.T) {
		ctx := &AuthContext{
			Tenant:      "Tenant", // Capital T
			AllowedNS:   []string{"Ns"},
			Permissions: []Permission{PermissionKVRead},
		}

		// Case sensitive - should fail
		err := authorizer.Authorize(ctx, "tenant", "Ns", PermissionKVRead)
		assert.Error(t, err)

		err = authorizer.Authorize(ctx, "Tenant", "ns", PermissionKVRead)
		assert.Error(t, err)

		// Exact match should work
		err = authorizer.Authorize(ctx, "Tenant", "Ns", PermissionKVRead)
		assert.NoError(t, err)
	})

	t.Run("Authorize with very long tenant/namespace names", func(t *testing.T) {
		longName := ""
		for i := 0; i < 1000; i++ {
			longName += "a"
		}

		ctx := &AuthContext{
			Tenant:      longName,
			AllowedNS:   []string{longName},
			Permissions: []Permission{PermissionKVRead},
		}

		err := authorizer.Authorize(ctx, longName, longName, PermissionKVRead)
		assert.NoError(t, err)
	})
}

func TestForbiddenError_EdgeCases(t *testing.T) {
	t.Run("ForbiddenError with all fields", func(t *testing.T) {
		err := ForbiddenError{
			Resource: "tenant/ns/resource",
			Action:   "read",
			Reason:   "no permission",
		}

		errMsg := err.Error()
		assert.Contains(t, errMsg, "tenant/ns/resource")
		assert.Contains(t, errMsg, "read")
		assert.Contains(t, errMsg, "no permission")
	})

	t.Run("ForbiddenError without reason", func(t *testing.T) {
		err := ForbiddenError{
			Resource: "tenant/ns/resource",
			Action:   "read",
		}

		errMsg := err.Error()
		assert.Contains(t, errMsg, "tenant/ns/resource")
		assert.Contains(t, errMsg, "read")
	})
}
