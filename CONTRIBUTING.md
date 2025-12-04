# Contributing to FlowMesh

Thank you for your interest in contributing to FlowMesh! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- **Go**: 1.21 or later ([installation guide](https://golang.org/doc/install))
- **Git**: Latest version
- **Make**: For running development commands
- **Docker**: Optional, for container-based development

### Initial Setup

1. **Fork and clone the repository**

```bash
git clone https://github.com/your-username/flowmesh.git
cd flowmesh
```

2. **Set up your development environment**

```bash
# Run the setup script
./scripts/dev-setup.sh

# Verify your environment
./scripts/verify.sh
```

3. **Install dependencies**

```bash
cd engine
go mod download
```

4. **Build the project**

```bash
make build
```

5. **Run tests**

```bash
make test
```

## Development Workflow

### Making Changes

1. **Create a branch**

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

2. **Make your changes**

   - Follow the code style guidelines below
   - Add tests for new functionality
   - Update documentation as needed

3. **Run tests and linters**

```bash
make test
make lint
```

4. **Commit your changes**

   - Write clear, descriptive commit messages
   - Follow conventional commits format when possible:
     - `feat: add new feature`
     - `fix: resolve bug`
     - `docs: update documentation`
     - `test: add tests`
     - `refactor: restructure code`

5. **Push and create a Pull Request**

```bash
git push origin feature/your-feature-name
```

Then create a PR on GitHub with a clear description of your changes.

## Code Style Guidelines

### Go Code Style

- Follow the [Effective Go](https://golang.org/doc/effective_go) guidelines
- Use `gofmt` to format code
- Use `golangci-lint` for linting (configuration in `.golangci.yml`)
- Maximum line length: 120 characters (when reasonable)
- Use descriptive variable and function names
- Add comments for exported functions and types
- Keep functions focused and small

### Project Structure

- `internal/` - Private packages, not for external use
- `pkg/` - Public packages that can be imported by other projects
- `cmd/` - Application entry points
- Tests should be in the same package with `_test.go` suffix

### Example

```go
// ProcessMessage processes a message from the queue.
// It validates the message, applies transformations, and returns the result.
func ProcessMessage(ctx context.Context, msg *Message) (*Result, error) {
    // Implementation
}
```

## Testing

### Writing Tests

- Write unit tests for all new functionality
- Use table-driven tests when appropriate
- Test both success and error cases
- Use descriptive test names: `TestFunctionName_Scenario_ExpectedResult`

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
go test -v -cover ./...

# Run tests for a specific package
go test ./internal/config
```

### Test Coverage

- Aim for high test coverage (80%+ for critical paths)
- Focus on testing business logic and error handling
- Mock external dependencies when appropriate

## Documentation

### Code Documentation

- Document all exported functions, types, and packages
- Use clear, concise comments
- Include examples for complex functions
- Follow Go documentation conventions

### User Documentation

- Update README.md for user-facing changes
- Add/update docs in `docs/` directory
- Include examples when adding new features
- Keep documentation in sync with code changes

## Pull Request Process

1. **Update documentation** for any user-facing changes
2. **Add tests** for new functionality
3. **Ensure all tests pass** and code is properly formatted
4. **Update CHANGELOG.md** with a summary of changes
5. **Create a clear PR description** explaining:
   - What changed and why
   - How to test the changes
   - Any breaking changes

### PR Checklist

- [ ] Code follows the style guidelines
- [ ] Tests pass locally
- [ ] Tests added/updated for new functionality
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No merge conflicts

## Review Process

- PRs require at least one maintainer approval
- Address review comments promptly
- Be open to feedback and suggestions
- Keep discussions constructive and respectful

## Reporting Bugs

### Before Reporting

1. Check existing issues to see if the bug is already reported
2. Verify you're using the latest version
3. Try to reproduce the issue consistently

### Bug Report Template

When opening a bug report, please include:

- **Description**: Clear description of the bug
- **Steps to Reproduce**: Detailed steps to reproduce
- **Expected Behavior**: What should happen
- **Actual Behavior**: What actually happens
- **Environment**: OS, Go version, FlowMesh version
- **Logs**: Relevant log output
- **Additional Context**: Screenshots, configuration files, etc.

## Feature Requests

We welcome feature requests! When suggesting a feature:

- Check existing issues and roadmap first
- Explain the problem the feature would solve
- Describe the proposed solution
- Discuss potential alternatives
- Consider implementation complexity

## Getting Help

- **Documentation**: Check the [docs/](docs/) directory
- **Issues**: Search existing issues on GitHub
- **Discussions**: Open a discussion for questions
- **Community**: Join our community channels (coming soon)

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please:

- Be respectful and considerate
- Accept constructive criticism gracefully
- Focus on what is best for the community
- Show empathy towards others

## Recognition

Contributors will be recognized in:
- CONTRIBUTORS.md file
- Release notes
- Project documentation

Thank you for contributing to FlowMesh! 🎉

