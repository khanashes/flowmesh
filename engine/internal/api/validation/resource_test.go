package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseResourcePath(t *testing.T) {
	t.Run("valid path", func(t *testing.T) {
		tenant, namespace, resourceType, name, err := ParseResourcePath("tenant1/ns1/stream/my-stream")
		require.NoError(t, err)
		assert.Equal(t, "tenant1", tenant)
		assert.Equal(t, "ns1", namespace)
		assert.Equal(t, "stream", resourceType)
		assert.Equal(t, "my-stream", name)
	})

	t.Run("invalid path - wrong number of components", func(t *testing.T) {
		_, _, _, _, err := ParseResourcePath("tenant1/ns1/stream")
		assert.Error(t, err)
		assert.IsType(t, ResourcePathError{}, err)
	})

	t.Run("invalid resource type", func(t *testing.T) {
		_, _, _, _, err := ParseResourcePath("tenant1/ns1/invalid/my-resource")
		assert.Error(t, err)
		assert.IsType(t, ResourcePathError{}, err)
	})
}

func TestBuildResourcePath(t *testing.T) {
	t.Run("valid components", func(t *testing.T) {
		path, err := BuildResourcePath("tenant1", "ns1", "stream", "my-stream")
		require.NoError(t, err)
		assert.Equal(t, "tenant1/ns1/stream/my-stream", path)
	})

	t.Run("invalid resource type", func(t *testing.T) {
		_, err := BuildResourcePath("tenant1", "ns1", "invalid", "my-resource")
		assert.Error(t, err)
	})
}

func TestValidateResourcePathComponents(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant1", "ns1", "stream", "my-stream")
		assert.NoError(t, err)
	})

	t.Run("empty tenant", func(t *testing.T) {
		err := ValidateResourcePathComponents("", "ns1", "stream", "my-stream")
		assert.Error(t, err)
	})

	t.Run("invalid resource type", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant1", "ns1", "invalid", "my-resource")
		assert.Error(t, err)
	})
}
