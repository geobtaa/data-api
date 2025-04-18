.PHONY: lint lint-black lint-flake8 test

lint: lint-black lint-flake8

lint-black:
	@echo "Linting with Black..."
	black .

lint-flake8:
	@echo "Linting with Flake8..."
	flake8 app/ tests/ scripts/

test: lint-flake8
	@echo "Running tests..."
	pytest 