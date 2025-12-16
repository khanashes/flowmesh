package filter

import (
	"testing"
)

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		ctx      Context
		expected bool
	}{
		{
			name:     "simple equality",
			expr:     "foo == \"bar\"",
			ctx:      Context{"foo": "bar"},
			expected: true,
		},
		{
			name:     "simple inequality",
			expr:     "foo != \"baz\"",
			ctx:      Context{"foo": "bar"},
			expected: true,
		},
		{
			name:     "number comparison",
			expr:     "age > 18",
			ctx:      Context{"age": 20},
			expected: true,
		},
		{
			name:     "property access",
			expr:     "headers.type == \"order\"",
			ctx:      Context{"headers": map[string]interface{}{"type": "order"}},
			expected: true,
		},
		{
			name:     "logical and",
			expr:     "foo == \"bar\" && age > 18",
			ctx:      Context{"foo": "bar", "age": 20},
			expected: true,
		},
		{
			name:     "logical or",
			expr:     "foo == \"baz\" || age > 18",
			ctx:      Context{"foo": "bar", "age": 20},
			expected: true,
		},
		{
			name:     "contains",
			expr:     "tags contains \"urgent\"",
			ctx:      Context{"tags": "urgent,important"},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := Parse(tc.expr)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			result, err := expr.Evaluate(tc.ctx)
			if err != nil {
				t.Fatalf("Evaluate error: %v", err)
			}

			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}
