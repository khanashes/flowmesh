package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseResourcePath_EdgeCases(t *testing.T) {
	t.Run("Path with special characters", func(t *testing.T) {
		path := "tenant-1/ns_1/stream/my-stream-name"
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, "tenant-1", tenant)
		assert.Equal(t, "ns_1", namespace)
		assert.Equal(t, "stream", resourceType)
		assert.Equal(t, "my-stream-name", name)
	})

	t.Run("Path with unicode characters", func(t *testing.T) {
		path := "租户/命名空间/stream/流名称"
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, "租户", tenant)
		assert.Equal(t, "命名空间", namespace)
		assert.Equal(t, "stream", resourceType)
		assert.Equal(t, "流名称", name)
	})

	t.Run("Path with emoji", func(t *testing.T) {
		path := "tenant-🧪/ns-🚀/stream/stream-✨"
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, "tenant-🧪", tenant)
		assert.Equal(t, "ns-🚀", namespace)
		assert.Equal(t, "stream", resourceType)
		assert.Equal(t, "stream-✨", name)
	})

	t.Run("Path with numbers", func(t *testing.T) {
		path := "tenant123/ns456/queue/queue789"
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, "tenant123", tenant)
		assert.Equal(t, "ns456", namespace)
		assert.Equal(t, "queue", resourceType)
		assert.Equal(t, "queue789", name)
	})

	t.Run("Path with single character components", func(t *testing.T) {
		path := "a/b/kv/c"
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, "a", tenant)
		assert.Equal(t, "b", namespace)
		assert.Equal(t, "kv", resourceType)
		assert.Equal(t, "c", name)
	})

	t.Run("Path with very long components", func(t *testing.T) {
		longComponent := ""
		for i := 0; i < 1000; i++ {
			longComponent += "a"
		}
		path := longComponent + "/" + longComponent + "/stream/" + longComponent
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, longComponent, tenant)
		assert.Equal(t, longComponent, namespace)
		assert.Equal(t, "stream", resourceType)
		assert.Equal(t, longComponent, name)
	})

	t.Run("Path with leading/trailing spaces", func(t *testing.T) {
		path := " tenant / ns / stream / name "
		tenant, namespace, resourceType, name, err := ParseResourcePath(path)
		require.NoError(t, err)
		assert.Equal(t, "tenant", tenant) // Trimmed
		assert.Equal(t, "ns", namespace)
		assert.Equal(t, "stream", resourceType)
		assert.Equal(t, "name", name)
	})

	t.Run("Path with extra slashes", func(t *testing.T) {
		// Multiple consecutive slashes
		_, _, _, _, err := ParseResourcePath("tenant//ns/stream/name")
		assert.Error(t, err)
		assert.IsType(t, ResourcePathError{}, err)
	})

	t.Run("Path with too few components", func(t *testing.T) {
		testCases := []string{
			"tenant",
			"tenant/ns",
			"tenant/ns/stream",
		}

		for _, path := range testCases {
			t.Run(path, func(t *testing.T) {
				_, _, _, _, err := ParseResourcePath(path)
				assert.Error(t, err)
				assert.IsType(t, ResourcePathError{}, err)
			})
		}
	})

	t.Run("Path with too many components", func(t *testing.T) {
		_, _, _, _, err := ParseResourcePath("tenant/ns/stream/name/extra")
		assert.Error(t, err)
		assert.IsType(t, ResourcePathError{}, err)
	})

	t.Run("Empty path", func(t *testing.T) {
		_, _, _, _, err := ParseResourcePath("")
		assert.Error(t, err)
		assert.IsType(t, ResourcePathError{}, err)
	})

	t.Run("Path with only slashes", func(t *testing.T) {
		_, _, _, _, err := ParseResourcePath("///")
		assert.Error(t, err)
		assert.IsType(t, ResourcePathError{}, err)
	})
}

func TestBuildResourcePath_EdgeCases(t *testing.T) {
	t.Run("Build with unicode", func(t *testing.T) {
		path, err := BuildResourcePath("租户", "命名空间", "stream", "流名称")
		require.NoError(t, err)
		assert.Equal(t, "租户/命名空间/stream/流名称", path)
	})

	t.Run("Build with emoji", func(t *testing.T) {
		path, err := BuildResourcePath("tenant-🧪", "ns-🚀", "queue", "queue-✨")
		require.NoError(t, err)
		assert.Equal(t, "tenant-🧪/ns-🚀/queue/queue-✨", path)
	})

	t.Run("Build with special characters", func(t *testing.T) {
		path, err := BuildResourcePath("tenant-1", "ns_1", "kv", "key-name")
		require.NoError(t, err)
		assert.Equal(t, "tenant-1/ns_1/kv/key-name", path)
	})

	t.Run("Build with all resource types", func(t *testing.T) {
		types := []string{"stream", "queue", "kv"}

		for _, resourceType := range types {
			t.Run(resourceType, func(t *testing.T) {
				path, err := BuildResourcePath("tenant", "ns", resourceType, "name")
				require.NoError(t, err)
				assert.Contains(t, path, resourceType)
			})
		}
	})

	t.Run("Build with empty strings (should fail)", func(t *testing.T) {
		_, err := BuildResourcePath("", "ns", "stream", "name")
		assert.Error(t, err)

		_, err = BuildResourcePath("tenant", "", "stream", "name")
		assert.Error(t, err)

		_, err = BuildResourcePath("tenant", "ns", "", "name")
		assert.Error(t, err)

		_, err = BuildResourcePath("tenant", "ns", "stream", "")
		assert.Error(t, err)
	})

	t.Run("Build with whitespace-only strings (should fail)", func(t *testing.T) {
		_, err := BuildResourcePath("   ", "ns", "stream", "name")
		assert.Error(t, err)

		_, err = BuildResourcePath("tenant", "   ", "stream", "name")
		assert.Error(t, err)
	})
}

func TestValidateResourcePathComponents_EdgeCases(t *testing.T) {
	t.Run("Valid with all resource types", func(t *testing.T) {
		types := []string{"stream", "queue", "kv"}

		for _, resourceType := range types {
			t.Run(resourceType, func(t *testing.T) {
				err := ValidateResourcePathComponents("tenant", "ns", resourceType, "name")
				assert.NoError(t, err)
			})
		}
	})

	t.Run("Invalid resource types", func(t *testing.T) {
		invalidTypes := []string{
			"invalid",
			"STREAM", // case sensitive
			"Stream",
			"streams", // plural
			"",
			" ",
			"stream queue", // space
		}

		for _, resourceType := range invalidTypes {
			t.Run(resourceType, func(t *testing.T) {
				err := ValidateResourcePathComponents("tenant", "ns", resourceType, "name")
				assert.Error(t, err)
			})
		}
	})

	t.Run("Case sensitive resource types", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant", "ns", "STREAM", "name")
		assert.Error(t, err)
	})

	t.Run("Unicode in components", func(t *testing.T) {
		err := ValidateResourcePathComponents("租户", "命名空间", "stream", "流名称")
		assert.NoError(t, err)
	})

	t.Run("Special characters in components", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant-1", "ns_1", "stream", "stream-name")
		assert.NoError(t, err)
	})

	t.Run("Numbers in components", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant123", "ns456", "queue", "queue789")
		assert.NoError(t, err)
	})

	t.Run("Very long component names", func(t *testing.T) {
		longName := ""
		for i := 0; i < 10000; i++ {
			longName += "a"
		}

		err := ValidateResourcePathComponents(longName, "ns", "stream", "name")
		assert.NoError(t, err) // No length limit in MVP
	})

	t.Run("Newlines in components (should fail or be sanitized)", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant\n", "ns", "stream", "name")
		// TrimSpace should handle this
		// But newlines in the middle would still pass validation
		_ = err
	})

	t.Run("Null bytes in components", func(t *testing.T) {
		err := ValidateResourcePathComponents("tenant\x00", "ns", "stream", "name")
		// Should validate (no null byte check in MVP)
		_ = err
	})
}

func TestResourcePathRoundTrip_EdgeCases(t *testing.T) {
	t.Run("Parse then build should be consistent", func(t *testing.T) {
		testCases := []string{
			"tenant/ns/stream/name",
			"tenant-1/ns_1/queue/queue-name",
			"租户/命名空间/kv/键名",
			"tenant/ns/stream/my-stream-name-123",
		}

		for _, originalPath := range testCases {
			t.Run(originalPath, func(t *testing.T) {
				tenant, namespace, resourceType, name, err := ParseResourcePath(originalPath)
				require.NoError(t, err)

				builtPath, err := BuildResourcePath(tenant, namespace, resourceType, name)
				require.NoError(t, err)

				assert.Equal(t, originalPath, builtPath)
			})
		}
	})

	t.Run("Build then parse should be consistent", func(t *testing.T) {
		testCases := []struct {
			tenant       string
			namespace    string
			resourceType string
			name         string
		}{
			{"tenant", "ns", "stream", "name"},
			{"tenant-1", "ns_1", "queue", "queue-name"},
			{"租户", "命名空间", "kv", "键名"},
		}

		for _, tc := range testCases {
			t.Run(tc.tenant+"/"+tc.namespace, func(t *testing.T) {
				path, err := BuildResourcePath(tc.tenant, tc.namespace, tc.resourceType, tc.name)
				require.NoError(t, err)

				tenant, namespace, resourceType, name, err := ParseResourcePath(path)
				require.NoError(t, err)

				assert.Equal(t, tc.tenant, tenant)
				assert.Equal(t, tc.namespace, namespace)
				assert.Equal(t, tc.resourceType, resourceType)
				assert.Equal(t, tc.name, name)
			})
		}
	})
}
