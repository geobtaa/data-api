.PHONY: lint lint-check format test lint-test

# Run both linting and formatting checks (without modifying files)
lint:
	@echo "Checking code with ruff..."
	ruff check app/ tests/ scripts/

# Format code in-place
format:
	@echo "Formatting code with ruff..."
	ruff format app/ tests/ scripts/
	ruff check --fix app/ tests/ scripts/

# Check formatting only (for CI)
lint-check:
	@echo "Checking formatting with ruff..."
	ruff format --check app/ tests/ scripts/
	ruff check app/ tests/ scripts/

# Run just the tests
test:
	@echo "Running tests..."
	pytest --full-trace

# Run linting and then tests (for CI)
lint-test: lint-check test 