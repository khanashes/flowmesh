package auth

// Permission represents an API permission
type Permission string

const (
	// KV permissions
	PermissionKVRead  Permission = "kv.read"
	PermissionKVWrite Permission = "kv.write"

	// Queue permissions
	PermissionQueueRead  Permission = "queue.read"
	PermissionQueueWrite Permission = "queue.write"

	// Stream permissions
	PermissionStreamRead  Permission = "stream.read"
	PermissionStreamWrite Permission = "stream.write"
)

// APIToken represents an API token with its metadata
type APIToken struct {
	// TokenHash is the hashed token value (never store plain tokens)
	TokenHash string
	// Tenant is the tenant this token belongs to
	Tenant string
	// AllowedNS is the list of allowed namespaces (empty means all)
	AllowedNS []string
	// Permissions is the list of permissions granted
	Permissions []Permission
	// CreatedAt is when the token was created
	CreatedAt int64 // Unix timestamp
	// ExpiresAt is when the token expires (0 means no expiration)
	ExpiresAt int64 // Unix timestamp
}

// HasPermission checks if the token has a specific permission
func (t *APIToken) HasPermission(perm Permission) bool {
	for _, p := range t.Permissions {
		if p == perm {
			return true
		}
	}
	return false
}

// IsNamespaceAllowed checks if the token allows access to a namespace
func (t *APIToken) IsNamespaceAllowed(namespace string) bool {
	// Empty AllowedNS means all namespaces are allowed
	if len(t.AllowedNS) == 0 {
		return true
	}
	for _, ns := range t.AllowedNS {
		if ns == namespace {
			return true
		}
	}
	return false
}

// IsExpired checks if the token is expired
func (t *APIToken) IsExpired(now int64) bool {
	if t.ExpiresAt == 0 {
		return false // No expiration
	}
	return now >= t.ExpiresAt
}

// AuthContext contains authentication and authorization context for a request
type AuthContext struct {
	// TokenHash is the hashed token that authenticated this request
	TokenHash string
	// Tenant is the tenant from the token
	Tenant string
	// AllowedNS are the allowed namespaces
	AllowedNS []string
	// Permissions are the granted permissions
	Permissions []Permission
}

// HasPermission checks if the auth context has a specific permission
func (c *AuthContext) HasPermission(perm Permission) bool {
	for _, p := range c.Permissions {
		if p == perm {
			return true
		}
	}
	return false
}

// IsNamespaceAllowed checks if the auth context allows access to a namespace
func (c *AuthContext) IsNamespaceAllowed(namespace string) bool {
	if len(c.AllowedNS) == 0 {
		return true
	}
	for _, ns := range c.AllowedNS {
		if ns == namespace {
			return true
		}
	}
	return false
}
