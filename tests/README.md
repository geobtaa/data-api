# Testing the BTAA Geoportal API

This directory contains tests for the BTAA Geoportal API. The tests are organized by component and use pytest as the test runner.

## Test Structure

- `tests/api/v1/`: Tests for the API v1 endpoints
- `tests/elasticsearch/`: Tests for Elasticsearch integration
- `tests/services/`: Tests for service classes
- `tests/gazetteer/`: Tests for gazetteer components
- `tests/viewers/`: Tests for viewer components

## Running Tests

To run all tests:

```bash
make test
```

To run specific tests:

```bash
# Run a specific test file
pytest tests/api/v1/test_document_endpoints.py -v

# Run a specific test function
pytest tests/api/v1/test_document_endpoints.py::test_get_document -v

# Run tests with a specific marker
pytest -m "asyncio" -v
```

## Test Configuration

The tests use fixtures defined in `conftest.py` files. These fixtures provide mock objects and data for testing.

### Key Fixtures

- `client`: A FastAPI test client
- `mock_document`: A mock document for testing
- `mock_elasticsearch_response`: A mock Elasticsearch response
- `mock_task`: A mock Celery task

## Testing Approach

The tests are designed to be:

1. **Isolated**: Tests should not depend on each other or external services
2. **Fast**: Tests should run quickly to enable rapid feedback
3. **Clear**: Tests should clearly show what they're testing and what the expected outcome is

Tests use mocking to avoid making actual database or API calls. This approach ensures that tests run quickly and reliably.

## Mock Data

Mock data for tests is defined in:

- Fixture files in `tests/fixtures/`
- Pytest fixtures in `conftest.py` files

## Adding New Tests

When adding new tests:

1. Create a new test file in the appropriate directory
2. Use existing fixtures when possible
3. Add new fixtures in the appropriate `conftest.py` file if needed
4. Follow the existing naming conventions

## Test Coverage

To generate a test coverage report:

```bash
make coverage
```

The report will be available in the `coverage_html_report` directory. 