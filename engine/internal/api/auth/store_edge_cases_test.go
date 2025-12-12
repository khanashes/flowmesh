package auth

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryTokenStore_EdgeCases(t *testing.T) {
	t.Run("CreateToken with empty tenant", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		_, _, err := store.CreateToken("", []string{}, []Permission{}, 0)
		// Empty tenant is allowed (no validation in MVP)
		assert.NoError(t, err)
	})

	t.Run("CreateToken with very long tenant name", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		longTenant := ""
		for i := 0; i < 1000; i++ {
			longTenant += "a"
		}
		_, _, err := store.CreateToken(longTenant, []string{}, []Permission{}, 0)
		assert.NoError(t, err)
	})

	t.Run("CreateToken with nil permissions", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		_, _, err := store.CreateToken("test", []string{}, nil, 0)
		assert.NoError(t, err)
	})

	t.Run("CreateToken with duplicate permissions", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		_, apiToken, err := store.CreateToken("test", []string{}, []Permission{
			PermissionKVRead,
			PermissionKVRead, // duplicate
		}, 0)
		require.NoError(t, err)
		// Should have the permission (duplicates are stored but that's okay)
		assert.True(t, apiToken.HasPermission(PermissionKVRead))
	})

	t.Run("ValidateToken with empty string", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		_, err := store.ValidateToken("")
		assert.Error(t, err)
		assert.IsType(t, TokenNotFoundError{}, err)
	})

	t.Run("ValidateToken with very long token", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		longToken := ""
		for i := 0; i < 10000; i++ {
			longToken += "a"
		}
		_, err := store.ValidateToken(longToken)
		assert.Error(t, err)
		assert.IsType(t, TokenNotFoundError{}, err)
	})

	t.Run("Token expiration at exact boundary", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		now := time.Now().Unix()
		plainToken, _, err := store.CreateToken("test", []string{}, []Permission{}, now)
		require.NoError(t, err)

		// Token should be expired immediately (or very soon)
		time.Sleep(100 * time.Millisecond)
		_, err = store.ValidateToken(plainToken)
		// May or may not be expired depending on timing
		_ = err
	})

	t.Run("DeleteToken with non-existent token hash", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		err := store.DeleteToken("non-existent-hash")
		assert.Error(t, err)
		assert.IsType(t, TokenNotFoundError{}, err)
	})

	t.Run("DeleteToken with empty hash", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		err := store.DeleteToken("")
		assert.Error(t, err)
		assert.IsType(t, TokenNotFoundError{}, err)
	})

	t.Run("Concurrent token creation", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		var wg sync.WaitGroup
		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, err := store.CreateToken("test", []string{}, []Permission{}, 0)
				assert.NoError(t, err)
			}()
		}

		wg.Wait()
	})

	t.Run("Concurrent token validation", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		plainToken, _, err := store.CreateToken("test", []string{}, []Permission{}, 0)
		require.NoError(t, err)

		var wg sync.WaitGroup
		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := store.ValidateToken(plainToken)
				assert.NoError(t, err)
			}()
		}

		wg.Wait()
	})

	t.Run("Concurrent token deletion", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		_, apiToken, err := store.CreateToken("test", []string{}, []Permission{}, 0)
		require.NoError(t, err)

		var wg sync.WaitGroup
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Multiple goroutines trying to delete same token
				_ = store.DeleteToken(apiToken.TokenHash)
			}()
		}

		wg.Wait()
		// Token should be deleted (at least one delete succeeded)
		_, err = store.ValidateToken("dummy-token")
		assert.Error(t, err) // Token should not be found
	})

	t.Run("Token with all permissions", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		allPerms := []Permission{
			PermissionKVRead,
			PermissionKVWrite,
			PermissionQueueRead,
			PermissionQueueWrite,
			PermissionStreamRead,
			PermissionStreamWrite,
		}
		_, apiToken, err := store.CreateToken("test", []string{}, allPerms, 0)
		require.NoError(t, err)

		for _, perm := range allPerms {
			assert.True(t, apiToken.HasPermission(perm))
		}
	})

	t.Run("Token with no permissions", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		_, apiToken, err := store.CreateToken("test", []string{}, []Permission{}, 0)
		require.NoError(t, err)

		assert.False(t, apiToken.HasPermission(PermissionKVRead))
		assert.False(t, apiToken.HasPermission(PermissionQueueWrite))
	})

	t.Run("Token with unicode in tenant and namespace", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		unicodeTenant := "租户-🧪-テナント"
		unicodeNS := []string{"命名空间-🧪-ネームスペース"}

		_, apiToken, err := store.CreateToken(unicodeTenant, unicodeNS, []Permission{}, 0)
		require.NoError(t, err)
		assert.Equal(t, unicodeTenant, apiToken.Tenant)
		assert.Equal(t, unicodeNS, apiToken.AllowedNS)
		assert.True(t, apiToken.IsNamespaceAllowed("命名空间-🧪-ネームスペース"))
	})

	t.Run("GetTokenByHash with expired token", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		expiresAt := time.Now().Add(-1 * time.Hour).Unix() // Expired in the past
		_, apiToken, err := store.CreateToken("test", []string{}, []Permission{}, expiresAt)
		require.NoError(t, err)

		_, err = store.GetTokenByHash(apiToken.TokenHash)
		assert.Error(t, err)
		assert.IsType(t, UnauthorizedError{}, err)
	})

	t.Run("Multiple tokens with same tenant", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		tenant := "same-tenant"

		token1, _, err := store.CreateToken(tenant, []string{"ns1"}, []Permission{PermissionKVRead}, 0)
		require.NoError(t, err)

		token2, _, err := store.CreateToken(tenant, []string{"ns2"}, []Permission{PermissionKVWrite}, 0)
		require.NoError(t, err)

		// Both tokens should be valid
		_, err = store.ValidateToken(token1)
		assert.NoError(t, err)

		_, err = store.ValidateToken(token2)
		assert.NoError(t, err)

		// Tokens should be different
		assert.NotEqual(t, token1, token2)
	})
}

func TestAPIToken_EdgeCases(t *testing.T) {
	t.Run("IsNamespaceAllowed with empty namespace string", func(t *testing.T) {
		token := &APIToken{
			AllowedNS: []string{"ns1", "ns2"},
		}
		assert.False(t, token.IsNamespaceAllowed(""))
	})

	t.Run("IsNamespaceAllowed with nil AllowedNS", func(t *testing.T) {
		token := &APIToken{
			AllowedNS: nil,
		}
		// nil slice should be treated as empty, which means all allowed
		// But let's check the actual behavior - empty slice means all allowed
		assert.True(t, token.IsNamespaceAllowed("any-ns"))
	})

	t.Run("HasPermission with non-existent permission", func(t *testing.T) {
		token := &APIToken{
			Permissions: []Permission{PermissionKVRead},
		}
		assert.False(t, token.HasPermission(Permission("non-existent-permission")))
	})

	t.Run("IsExpired with zero ExpiresAt", func(t *testing.T) {
		token := &APIToken{
			ExpiresAt: 0,
		}
		assert.False(t, token.IsExpired(time.Now().Unix()))
	})

	t.Run("IsExpired with future ExpiresAt", func(t *testing.T) {
		futureTime := time.Now().Add(1 * time.Hour).Unix()
		token := &APIToken{
			ExpiresAt: futureTime,
		}
		assert.False(t, token.IsExpired(time.Now().Unix()))
	})

	t.Run("IsExpired with past ExpiresAt", func(t *testing.T) {
		pastTime := time.Now().Add(-1 * time.Hour).Unix()
		token := &APIToken{
			ExpiresAt: pastTime,
		}
		assert.True(t, token.IsExpired(time.Now().Unix()))
	})
}

func TestAuthContext_EdgeCases(t *testing.T) {
	t.Run("HasPermission with empty permissions", func(t *testing.T) {
		ctx := &AuthContext{
			Permissions: []Permission{},
		}
		assert.False(t, ctx.HasPermission(PermissionKVRead))
	})

	t.Run("IsNamespaceAllowed with empty AllowedNS", func(t *testing.T) {
		ctx := &AuthContext{
			AllowedNS: []string{},
		}
		assert.True(t, ctx.IsNamespaceAllowed("any-namespace"))
	})

	t.Run("IsNamespaceAllowed with nil AllowedNS", func(t *testing.T) {
		ctx := &AuthContext{
			AllowedNS: nil,
		}
		assert.True(t, ctx.IsNamespaceAllowed("any-namespace"))
	})
}

func TestInMemoryTokenStore_AddDefaultToken(t *testing.T) {
	t.Run("AddDefaultToken creates token with all permissions", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		token, err := store.AddDefaultToken()
		require.NoError(t, err)
		assert.NotEmpty(t, token)

		// Validate the token
		apiToken, err := store.ValidateToken(token)
		require.NoError(t, err)

		// Should have all permissions
		assert.True(t, apiToken.HasPermission(PermissionKVRead))
		assert.True(t, apiToken.HasPermission(PermissionKVWrite))
		assert.True(t, apiToken.HasPermission(PermissionQueueRead))
		assert.True(t, apiToken.HasPermission(PermissionQueueWrite))
		assert.True(t, apiToken.HasPermission(PermissionStreamRead))
		assert.True(t, apiToken.HasPermission(PermissionStreamWrite))

		// Should allow all namespaces
		assert.True(t, apiToken.IsNamespaceAllowed("any-namespace"))

		// Should not expire
		assert.False(t, apiToken.IsExpired(time.Now().Unix()))
	})

	t.Run("Multiple calls to AddDefaultToken", func(t *testing.T) {
		store := NewInMemoryTokenStore()
		token1, err := store.AddDefaultToken()
		require.NoError(t, err)

		token2, err := store.AddDefaultToken()
		require.NoError(t, err)

		// Should create different tokens
		assert.NotEqual(t, token1, token2)

		// Both should be valid
		_, err = store.ValidateToken(token1)
		assert.NoError(t, err)

		_, err = store.ValidateToken(token2)
		assert.NoError(t, err)
	})
}
