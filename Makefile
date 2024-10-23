PACKAGE := megfile
VERSION := $(shell cat ${PACKAGE}/version.py | sed -n -E 's/.*=//; s/ //g; s/"//g; p')

test:
	pytest --cov-config=pyproject.toml --cov=${PACKAGE} --disable-socket --no-cov-on-fail --cov-report=html:html_cov/ --cov-report term-missing --cov-report=xml tests/ --durations=10

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
	bandit --format=sarif --recursive megfile/ > bandit-sarif.json || echo

pyre_check:
	pyre --output=json check > pyre-errors.json || echo
	cat pyre-errors.json | ./scripts/convert_results_to_sarif.py > pyre-sarif.json

mut:
	@echo Mutation testing...
	mutmut run || echo
	mutmut show all
	mutmut junitxml > mutmut.xml

doc:
	sphinx-build --fresh-env docs html_doc

release:
	git tag ${VERSION}
	git push origin ${VERSION}

	rm -rf build dist
	python3 -m build --wheel

	twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username='${PYPI_USERNAME}' --password='${PYPI_PASSWORD}' --repository-url 'http://pypi.i.brainpp.cn/r-eng/dev/'
	twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username='${PYPI_USERNAME_2}' --password='${PYPI_PASSWORD_2}' --repository-url 'https://pypi.megvii-inc.com/repository/pypi/'
	twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username=${PYPI_USERNAME_3} --password=${PYPI_PASSWORD_3}
