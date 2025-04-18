# Testing

This document describes how to run the test suite for the application.

## Prerequisites

Ensure you have the development dependencies installed. You can install them along with the package in editable mode using `uv`:

```bash
uv pip install -e '.[dev]'
```

## Running the Full Test Suite

To run all tests, including linting with Black, code coverage with pytest-cov, and the pytest suite itself, you can use the provided Makefile:

```bash
make test
```

Alternatively, you can run the commands individually:

1.  **Linting (Black):** Checks and formats the code according to Black standards.
    ```bash
    black .
    ```

2.  **Run Tests with Coverage (pytest & pytest-cov):** Executes the test suite and reports code coverage.
    ```bash
    pytest
    ```
    *(Note: Coverage is configured in `pytest.ini` to run automatically with `pytest`)*

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