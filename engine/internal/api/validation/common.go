package validation

import (
	"fmt"
	"strings"
)

// ValidateNonEmpty validates that a string is not empty
func ValidateNonEmpty(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s cannot be empty", field)
	}
	return nil
}

// ValidateResourcePathComponents validates tenant, namespace, resource type, and name
func ValidateResourcePathComponents(tenant, namespace, resourceType, name string) error {
	if err := ValidateNonEmpty("tenant", tenant); err != nil {
		return err
	}

	if err := ValidateNonEmpty("namespace", namespace); err != nil {
		return err
	}

	if err := ValidateNonEmpty("resource_type", resourceType); err != nil {
		return err
	}

	validTypes := map[string]bool{
		"stream": true,
		"queue":  true,
		"kv":     true,
	}
	if !validTypes[resourceType] {
		return fmt.Errorf("invalid resource type: %s (must be stream, queue, or kv)", resourceType)
	}

	if err := ValidateNonEmpty("name", name); err != nil {
		return err
	}

	return nil
}
