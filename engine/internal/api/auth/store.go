package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// TokenStore defines the interface for token storage
type TokenStore interface {
	// ValidateToken validates a token and returns the APIToken
	ValidateToken(token string) (*APIToken, error)
	// GetTokenByHash retrieves a token by its hash
	GetTokenByHash(tokenHash string) (*APIToken, error)
	// CreateToken creates a new token and returns the plain token and APIToken
	CreateToken(tenant string, allowedNS []string, permissions []Permission, expiresAt int64) (string, *APIToken, error)
	// DeleteToken deletes a token by its hash
	DeleteToken(tokenHash string) error
}

// InMemoryTokenStore is an in-memory implementation of TokenStore
type InMemoryTokenStore struct {
	tokens map[string]*APIToken // tokenHash -> APIToken
	mu     sync.RWMutex
}

// NewInMemoryTokenStore creates a new in-memory token store
func NewInMemoryTokenStore() *InMemoryTokenStore {
	return &InMemoryTokenStore{
		tokens: make(map[string]*APIToken),
	}
}

// hashToken hashes a token using SHA-256
func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

// ValidateToken validates a token and returns the APIToken
func (s *InMemoryTokenStore) ValidateToken(token string) (*APIToken, error) {
	tokenHash := hashToken(token)

	s.mu.RLock()
	defer s.mu.RUnlock()

	apiToken, exists := s.tokens[tokenHash]
	if !exists {
		return nil, TokenNotFoundError{TokenHash: tokenHash}
	}

	// Check expiration
	now := time.Now().Unix()
	if apiToken.IsExpired(now) {
		return nil, UnauthorizedError{Reason: "token expired"}
	}

	return apiToken, nil
}

// GetTokenByHash retrieves a token by its hash
func (s *InMemoryTokenStore) GetTokenByHash(tokenHash string) (*APIToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	apiToken, exists := s.tokens[tokenHash]
	if !exists {
		return nil, TokenNotFoundError{TokenHash: tokenHash}
	}

	// Check expiration
	now := time.Now().Unix()
	if apiToken.IsExpired(now) {
		return nil, UnauthorizedError{Reason: "token expired"}
	}

	return apiToken, nil
}

// CreateToken creates a new token and returns the plain token and APIToken
func (s *InMemoryTokenStore) CreateToken(tenant string, allowedNS []string, permissions []Permission, expiresAt int64) (string, *APIToken, error) {
	// Generate a new UUID-based token
	plainToken := uuid.New().String()

	tokenHash := hashToken(plainToken)
	now := time.Now().Unix()

	apiToken := &APIToken{
		TokenHash:   tokenHash,
		Tenant:      tenant,
		AllowedNS:   allowedNS,
		Permissions: permissions,
		CreatedAt:   now,
		ExpiresAt:   expiresAt,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tokens[tokenHash] = apiToken

	log.Info().
		Str("tenant", tenant).
		Int("permissions", len(permissions)).
		Msg("API token created")

	return plainToken, apiToken, nil
}

// DeleteToken deletes a token by its hash
func (s *InMemoryTokenStore) DeleteToken(tokenHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tokens[tokenHash]; !exists {
		return TokenNotFoundError{TokenHash: tokenHash}
	}

	delete(s.tokens, tokenHash)

	log.Info().
		Str("token_hash", tokenHash).
		Msg("API token deleted")

	return nil
}

// AddDefaultToken adds a default token for testing/development
func (s *InMemoryTokenStore) AddDefaultToken() (string, error) {
	// Create a token with all permissions and no expiration
	plainToken, _, err := s.CreateToken(
		"default",
		[]string{}, // Empty means all namespaces
		[]Permission{
			PermissionKVRead,
			PermissionKVWrite,
			PermissionQueueRead,
			PermissionQueueWrite,
			PermissionStreamRead,
			PermissionStreamWrite,
		},
		0, // No expiration
	)
	if err != nil {
		return "", fmt.Errorf("failed to create default token: %w", err)
	}

	log.Info().
		Str("token", plainToken).
		Msg("Default API token created (for development/testing)")

	return plainToken, nil
}
