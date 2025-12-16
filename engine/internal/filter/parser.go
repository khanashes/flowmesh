package filter

import (
	"fmt"
	"strings"
	"text/scanner"
)

// Parse parses a filter expression string into an implementation of Expression
// Parse parses a filter expression string into an implementation of Expression
func Parse(expr string) (Expression, error) {
	if expr == "" {
		return nil, nil
	}

	var s scanner.Scanner
	s.Init(strings.NewReader(expr))
	s.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings
	// Don't skip unexpected characters, we handle them
	s.Error = func(s *scanner.Scanner, msg string) {}

	p := &parser{s: &s}
	p.next()

	res, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	if p.tok != scanner.EOF {
		return nil, fmt.Errorf("unexpected token at end of expression: %s", p.lit)
	}

	return res, nil
}

type parser struct {
	s   *scanner.Scanner
	tok rune
	lit string
}

func (p *parser) next() {
	p.tok = p.s.Scan()
	p.lit = p.s.TokenText()

	// Handle multi-character operators
	switch p.tok {
	case '=':
		if p.s.Peek() == '=' {
			p.s.Scan()
			p.lit = "=="
			p.tok = -1 // Special token for operators
		}
	case '!':
		if p.s.Peek() == '=' {
			p.s.Scan()
			p.lit = "!="
			p.tok = -1
		}
	case '<':
		if p.s.Peek() == '=' {
			p.s.Scan()
			p.lit = "<="
			p.tok = -1
		}
	case '>':
		if p.s.Peek() == '=' {
			p.s.Scan()
			p.lit = ">="
			p.tok = -1
		}
	case '&':
		if p.s.Peek() == '&' {
			p.s.Scan()
			p.lit = "&&"
			p.tok = -1
		}
	case '|':
		if p.s.Peek() == '|' {
			p.s.Scan()
			p.lit = "||"
			p.tok = -1
		}
	}
}

func (p *parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expression, error) {
	lhs, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.lit == "||" {
		op := OpOr
		p.next()
		rhs, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		lhs = &BinaryExpression{Left: lhs, Operator: op, Right: rhs}
	}

	return lhs, nil
}

func (p *parser) parseAnd() (Expression, error) {
	lhs, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.lit == "&&" {
		op := OpAnd
		p.next()
		rhs, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		lhs = &BinaryExpression{Left: lhs, Operator: op, Right: rhs}
	}

	return lhs, nil
}

func (p *parser) parseComparison() (Expression, error) {
	lhs, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	switch p.lit {
	case "==", "!=", ">", "<", ">=", "<=", "contains":
		op := Operator(p.lit)
		p.next()
		rhs, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpression{Left: lhs, Operator: op, Right: rhs}, nil
	}

	return lhs, nil
}

func (p *parser) parsePrimary() (Expression, error) {
	switch p.tok {
	case scanner.Ident:
		name := p.lit
		p.next()
		if p.lit == "." {
			// Property access
			p.next()
			if p.tok != scanner.Ident {
				return nil, fmt.Errorf("expected identifier after dot")
			}
			prop := p.lit
			p.next()
			return &PropertyAccess{Object: &Identifier{Name: name}, Property: prop}, nil
		}
		return &Identifier{Name: name}, nil
	case scanner.String:
		val := strings.Trim(p.lit, "\"")
		p.next()
		return &Literal{Value: val, Type: TypeString}, nil
	case scanner.Int, scanner.Float:
		val := p.lit
		p.next()
		return &Literal{Value: val, Type: TypeNumber}, nil
	case '(': // '('
		p.next()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if p.lit != ")" {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		p.next()
		return expr, nil
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.lit)
	}
}
