VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip
PROJECT_ROOT = dune_aws


$(VENV)/bin/activate: requirements/dev.txt
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements/dev.txt


install:
	make $(VENV)/bin/activate

clean:
	rm -rf __pycache__
	rm -rf .mypy_cache
	rm -rf .pytest_cache

fmt:
	black ./

lint:
	pylint ${PROJECT_ROOT}/

types:
	mypy ${PROJECT_ROOT}/ --strict

check:
	make fmt
	make lint
	make types

test-unit:
	python -m pytest tests/unit

test-integration:
	python -m pytest tests/integration

test:
	python -m pytest tests

