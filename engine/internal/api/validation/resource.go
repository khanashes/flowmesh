package validation

import (
	"fmt"
	"strings"
)

// ResourcePathError indicates an invalid resource path
type ResourcePathError struct {
	Path   string
	Reason string
}

func (e ResourcePathError) Error() string {
	return fmt.Sprintf("invalid resource path '%s': %s", e.Path, e.Reason)
}

// ParseResourcePath parses a resource path string into components
// Format: tenant/namespace/resource_type/name
func ParseResourcePath(path string) (tenant, namespace, resourceType, name string, err error) {
	parts := strings.Split(path, "/")
	if len(parts) != 4 {
		return "", "", "", "", ResourcePathError{
			Path:   path,
			Reason: "resource path must have 4 components: tenant/namespace/resource_type/name",
		}
	}

	tenant = strings.TrimSpace(parts[0])
	namespace = strings.TrimSpace(parts[1])
	resourceType = strings.TrimSpace(parts[2])
	name = strings.TrimSpace(parts[3])

	if err := ValidateResourcePathComponents(tenant, namespace, resourceType, name); err != nil {
		return "", "", "", "", ResourcePathError{
			Path:   path,
			Reason: err.Error(),
		}
	}

	return tenant, namespace, resourceType, name, nil
}

// BuildResourcePath builds a resource path from components
func BuildResourcePath(tenant, namespace, resourceType, name string) (string, error) {
	if err := ValidateResourcePathComponents(tenant, namespace, resourceType, name); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%s/%s", tenant, namespace, resourceType, name), nil
}
