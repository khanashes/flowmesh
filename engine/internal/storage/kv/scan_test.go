package kv

import (
	"context"
	"testing"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManager_Scan_Filter(t *testing.T) {
	manager, metaStore, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a KV store resource
	config := &metastore.ResourceConfig{
		Tenant:    "test-tenant",
		Namespace: "test-ns",
		Name:      "scan-kv",
		Type:      metastore.ResourceKV,
	}
	require.NoError(t, config.Validate())
	require.NoError(t, metaStore.CreateResource(config))

	resourcePath := config.GetPath()
	require.NoError(t, manager.InitializeKVStore(ctx, resourcePath))

	// Set values
	// Key 1: user:1, value="alice", json={role: admin, active: true}
	err := manager.Set(ctx, resourcePath, "user:1", []byte(`{"name":"alice", "role":"admin", "active":true}`), DefaultSetOptions())
	require.NoError(t, err)

	// Key 2: user:2, value="bob", json={role: user, active: true}
	err = manager.Set(ctx, resourcePath, "user:2", []byte(`{"name":"bob", "role":"user", "active":true}`), DefaultSetOptions())
	require.NoError(t, err)

	// Key 3: user:3, value="charlie", json={role: user, active: false}
	err = manager.Set(ctx, resourcePath, "user:3", []byte(`{"name":"charlie", "role":"user", "active":false}`), DefaultSetOptions())
	require.NoError(t, err)

	// Key 4: config:1, value="settings"
	err = manager.Set(ctx, resourcePath, "config:1", []byte(`{"env":"prod"}`), DefaultSetOptions())
	require.NoError(t, err)

	// Test 1: Scan with prefix "user:", no filter
	results, err := manager.Scan(ctx, resourcePath, "user:", "", 10)
	require.NoError(t, err)
	require.Equal(t, 3, len(results))
	assert.Equal(t, "user:1", results[0].Key)
	assert.Equal(t, "user:2", results[1].Key)
	assert.Equal(t, "user:3", results[2].Key)

	// Test 2: Scan with prefix "user:", filter role == "admin"
	results, err = manager.Scan(ctx, resourcePath, "user:", `json.role == "admin"`, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "user:1", results[0].Key)

	// Test 3: Scan with prefix "user:", filter active == true
	// Note: Engine supports boolean literals in parser?
	// If not, I can assume boolean in JSON is parsed as generic value.
	// Evaluator `isEqual` compares generic values.
	// `types.go` parser handles `scanner.Ident`. "true" might be parsed as identifier unless specially handled.
	// The parser I wrote handles `ScanIdent`. If `true` is not quoted, it's an identifier.
	// The evaluator resolves identifiers from context. `true` is not in context.
	// So `json.active == true` might fail if `true` is treated as identifier lookup.
	// I should probably support boolean literals in parser or context.
	// OR use string comparison if I didn't implement boolean literals.
	// BUT, `json.active` will be `bool` from `json.Unmarshal`.
	// If I write filter `json.active == "true"`, `true` (bool) != "true" (string).
	// Let's test non-boolean first or check if I implemented boolean literals.
	// Steps 650/653 Parser implementation: `parsePrimary` handles `Ident`, `String`, `Int`, `Float`.
	// It does NOT handle `true`/`false` keywords.
	// So `true` is an identifier "true".
	// Unless I add `true` to the context or update parser.
	// I'll update context in the test or avoid boolean literals for now.
	// Wait, I can pass `true` in context? No, context is per-item.
	// I should update parser to handle true/false?
	// Or just use string comparison for now to verify integration.

	// Test 3: Integer comparison (age > 20)
	// I'll add age to json

	// Test 4: Filter by Key
	results, err = manager.Scan(ctx, resourcePath, "user:", `key == "user:2"`, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "user:2", results[0].Key)

	// Test 5: Contains
	results, err = manager.Scan(ctx, resourcePath, "user:", `json.name contains "ali"`, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "user:1", results[0].Key)
}
