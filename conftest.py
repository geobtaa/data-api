

def pytest_configure(config):
    config.addinivalue_line("addopts", "--cov=app --cov-report=term-missing --cov-report=html")
