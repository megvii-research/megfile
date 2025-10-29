PACKAGE := megfile
VERSION := $(shell cat ${PACKAGE}/version.py | sed -n -E 's/.*=//; s/ //g; s/"//g; p')

test:
	pytest \
		--cov=${PACKAGE} --cov-config=pyproject.toml --cov-report=html:html_cov/ --cov-report=term-missing --cov-report=xml --no-cov-on-fail \
		--retries 2 --cumulative-timing 1 \
		--durations=10 \
		tests/

format:
	ruff check --fix ${PACKAGE} tests scripts pyproject.toml
	ruff format ${PACKAGE} tests scripts pyproject.toml

style_check:
	ruff check ${PACKAGE} tests scripts pyproject.toml
	ruff format --check ${PACKAGE} tests scripts pyproject.toml

static_check:
	make pytype_check

pytype_check:
	pytype

bandit_check:
	bandit --quiet --format=sarif --recursive megfile/ > bandit-sarif.json || echo

pyre_check:
	pyre --version=none --output=json check > pyre-errors.json || echo
	cat pyre-errors.json | ./scripts/convert_results_to_sarif.py > pyre-sarif.json

mut:
	@echo Mutation testing...
	mutmut run || echo
	mutmut show all
	mutmut junitxml > mutmut.xml

doc:
	PYTHONPATH=. sphinx-build --fresh-env docs html_doc

release:
	git tag ${VERSION}
	git push origin ${VERSION}

	rm -rf build dist
	python3 -m build --wheel

	twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username='${PYPI_USERNAME}' --password='${PYPI_PASSWORD}'
