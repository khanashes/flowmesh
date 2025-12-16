package filter

// Operator represents a logical or comparison operator
type Operator string

const (
	OpEqual          Operator = "=="
	OpNotEqual       Operator = "!="
	OpGreaterThan    Operator = ">"
	OpLessThan       Operator = "<"
	OpGreaterOrEqual Operator = ">="
	OpLessOrEqual    Operator = "<="
	OpContains       Operator = "contains"
	OpAnd            Operator = "&&"
	OpOr             Operator = "||"
)

// ValueType represents the type of a value
type ValueType string

const (
	TypeString  ValueType = "string"
	TypeNumber  ValueType = "number"
	TypeBoolean ValueType = "boolean"
	TypeNull    ValueType = "null"
)

// Expression is the interface for all AST nodes
type Expression interface {
	Evaluate(ctx Context) (interface{}, error)
}

// Context represents the evaluation context (variables)
type Context map[string]interface{}

// BinaryExpression represents a binary operation (e.g., A == B)
type BinaryExpression struct {
	Left     Expression
	Operator Operator
	Right    Expression
}

// Literal represents a constant value
type Literal struct {
	Value interface{}
	Type  ValueType
}

func (l *Literal) Evaluate(ctx Context) (interface{}, error) {
	return l.Value, nil
}

// Identifier represents a variable lookup
type Identifier struct {
	Name string
}

func (i *Identifier) Evaluate(ctx Context) (interface{}, error) {
	if val, ok := ctx[i.Name]; ok {
		return val, nil
	}
	return nil, nil // Treat missing as null
}

// PropertyAccess represents accessing a nested property (e.g., headers.type)
type PropertyAccess struct {
	Object   Expression
	Property string
}

func (p *PropertyAccess) Evaluate(ctx Context) (interface{}, error) {
	obj, err := p.Object.Evaluate(ctx)
	if err != nil {
		return nil, err
	}

	if obj == nil {
		return nil, nil
	}

	// Handle map access
	if m, ok := obj.(map[string]interface{}); ok {
		if val, ok := m[p.Property]; ok {
			return val, nil
		}
		// Also support map[string]string
	} else if m, ok := obj.(map[string]string); ok {
		if val, ok := m[p.Property]; ok {
			return val, nil
		}
	}

	return nil, nil
}
