checkfiles = rearq/ tests/ examples/ conftest.py
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1

up:
	@poetry update

deps:
	@poetry install -E mysql -E postgres

style: deps
	@isort -src $(checkfiles)
	@black $(black_opts) $(checkfiles)

check: deps
	@black --check $(black_opts) $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@ruff --fix $(checkfiles)
	#mypy $(checkfiles)

test: deps
	$(py_warn) pytest

build: deps
	@rm -rf dist
	@poetry build

ci: deps check
