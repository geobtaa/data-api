# Testing

This document describes how to run the test suite for the application.

## Prerequisites

Ensure you have the development dependencies installed. You can install them along with the package in editable mode using `uv`:

```bash
uv pip install -e '.[dev]'
```

## Running the Full Test Suite

To run all tests, including linting with Ruff, code coverage with pytest-cov, and the pytest suite itself, you can use the provided Makefile:

```bash
make lint-test
```

This command will:
1. Check code formatting and linting with Ruff
2. Run the pytest suite with coverage reporting

## Code Quality Tools

The project uses Ruff for both linting and formatting:

### Linting

To check your code for issues without modifying files:

```bash
make lint
```

This runs Ruff's check command on the codebase.

### Formatting

To automatically format your code:

```bash
make format
```

This runs Ruff's format command and then applies any auto-fixes from the linter.

### Format Checking (CI)

To check if code is properly formatted without modifying files (useful for CI):

```bash
make lint-check
```

## Running Tests

You can run the test suite using:

```bash
make test
```

This executes pytest with the configuration in `pytest.ini`, which includes automatic coverage reporting.

## Running Individual Tests

You can run specific test files or even individual test functions using `pytest` arguments.

*   **Run a specific test file:**
    ```bash
    pytest tests/api/test_example.py
    ```

*   **Run tests in a specific directory:**
    ```bash
    pytest tests/services/
    ```

*   **Run a specific test function using the `-k` flag (keyword expression):**
    ```bash
    pytest -k test_example
    ```

*   **Run tests matching a specific marker (if you define markers later):**
    ```bash
    pytest -m <marker_name>
    ```

## Test Coverage

Coverage is automatically calculated when running `pytest` (as configured in `pytest.ini`). The report showing missing lines will be printed to the terminal. 