.PHONY: clean dev test

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

dev:
	pip install -e ".[local,test]"


test:
	pytest tests/unit --cov
