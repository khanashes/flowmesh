package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryTokenStore_CreateToken(t *testing.T) {
	store := NewInMemoryTokenStore()

	tenant := "test-tenant"
	allowedNS := []string{"ns1", "ns2"}
	permissions := []Permission{PermissionKVRead, PermissionKVWrite}

	plainToken, apiToken, err := store.CreateToken(tenant, allowedNS, permissions, 0)
	require.NoError(t, err)
	assert.NotEmpty(t, plainToken)
	assert.NotNil(t, apiToken)
	assert.Equal(t, tenant, apiToken.Tenant)
	assert.Equal(t, allowedNS, apiToken.AllowedNS)
	assert.Equal(t, permissions, apiToken.Permissions)
}

func TestInMemoryTokenStore_ValidateToken(t *testing.T) {
	store := NewInMemoryTokenStore()

	plainToken, _, err := store.CreateToken("test", []string{}, []Permission{PermissionKVRead}, 0)
	require.NoError(t, err)

	// Valid token
	apiToken, err := store.ValidateToken(plainToken)
	require.NoError(t, err)
	assert.NotNil(t, apiToken)

	// Invalid token
	_, err = store.ValidateToken("invalid-token")
	assert.Error(t, err)
	assert.IsType(t, TokenNotFoundError{}, err)
}

func TestInMemoryTokenStore_TokenExpiration(t *testing.T) {
	store := NewInMemoryTokenStore()

	// Create token with expiration in 1 second
	expiresAt := time.Now().Add(1 * time.Second).Unix()
	plainToken, _, err := store.CreateToken("test", []string{}, []Permission{PermissionKVRead}, expiresAt)
	require.NoError(t, err)

	// Token should be valid immediately
	_, err = store.ValidateToken(plainToken)
	require.NoError(t, err)

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Token should be expired
	_, err = store.ValidateToken(plainToken)
	assert.Error(t, err)
	assert.IsType(t, UnauthorizedError{}, err)
}

func TestInMemoryTokenStore_DeleteToken(t *testing.T) {
	store := NewInMemoryTokenStore()

	plainToken, apiToken, err := store.CreateToken("test", []string{}, []Permission{PermissionKVRead}, 0)
	require.NoError(t, err)

	// Delete token
	err = store.DeleteToken(apiToken.TokenHash)
	require.NoError(t, err)

	// Token should no longer be valid
	_, err = store.ValidateToken(plainToken)
	assert.Error(t, err)
	assert.IsType(t, TokenNotFoundError{}, err)
}

func TestAPIToken_HasPermission(t *testing.T) {
	token := &APIToken{
		Permissions: []Permission{PermissionKVRead, PermissionKVWrite},
	}

	assert.True(t, token.HasPermission(PermissionKVRead))
	assert.True(t, token.HasPermission(PermissionKVWrite))
	assert.False(t, token.HasPermission(PermissionQueueRead))
}

func TestAPIToken_IsNamespaceAllowed(t *testing.T) {
	t.Run("empty allowed namespaces means all allowed", func(t *testing.T) {
		token := &APIToken{
			AllowedNS: []string{},
		}
		assert.True(t, token.IsNamespaceAllowed("any-namespace"))
	})

	t.Run("specific namespaces", func(t *testing.T) {
		token := &APIToken{
			AllowedNS: []string{"ns1", "ns2"},
		}
		assert.True(t, token.IsNamespaceAllowed("ns1"))
		assert.True(t, token.IsNamespaceAllowed("ns2"))
		assert.False(t, token.IsNamespaceAllowed("ns3"))
	})
}
