checkfiles = rearq/ tests/ examples/ conftest.py
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1

help:
	@echo "ReArq development makefile"
	@echo
	@echo  "usage: make <target>"
	@echo  "Targets:"
	@echo  "    up			Updates dev/test dependencies"
	@echo  "    deps		Ensure dev/test dependencies are installed"
	@echo  "    check		Checks that build is sane"
	@echo  "    test		Runs all tests"
	@echo  "    style		Auto-formats the code"

up:
	@poetry update

deps:
	@poetry install

style: deps
	isort -rc $(checkfiles)
	black $(black_opts) $(checkfiles)

check: deps
ifneq ($(shell which black),)
	black --check $(black_opts) $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
endif
	flake8 $(checkfiles)
	mypy $(checkfiles)
	pylint -d C,W,R $(checkfiles)
	bandit -r $(checkfiles)
	python setup.py check -mrs

test: deps
	$(py_warn) py.test

build: deps
	@poetry build

publish: deps
	@poetry publish --build

docs:
	@pip install -r docs/requirements.txt
	@sphinx-build docs/ docs/_build

.PHONY: docs