package filter

import (
	"fmt"
	"strings"
)

// Evaluate evaluates a binary expression
func (e *BinaryExpression) Evaluate(ctx Context) (interface{}, error) {
	left, err := e.Left.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	right, err := e.Right.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	switch e.Operator {
	case OpEqual:
		return isEqual(left, right), nil
	case OpNotEqual:
		return !isEqual(left, right), nil
	case OpGreaterThan:
		return compare(left, right) > 0, nil
	case OpLessThan:
		return compare(left, right) < 0, nil
	case OpGreaterOrEqual:
		return compare(left, right) >= 0, nil
	case OpLessOrEqual:
		return compare(left, right) <= 0, nil
	case OpContains:
		return contains(left, right), nil
	case OpAnd:
		return toBool(left) && toBool(right), nil
	case OpOr:
		return toBool(left) || toBool(right), nil
	default:
		return nil, fmt.Errorf("unknown operator: %s", e.Operator)
	}
}

func isEqual(a, b interface{}) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func compare(a, b interface{}) int {
	// Simple string comparison for now. Ideally should handle types better.
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

func contains(container, item interface{}) bool {
	sContainer := fmt.Sprintf("%v", container)
	sItem := fmt.Sprintf("%v", item)
	return strings.Contains(sContainer, sItem)
}

func toBool(v interface{}) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	s := fmt.Sprintf("%v", v)
	return s != "" && s != "false" && s != "0" && s != "<nil>"
}
