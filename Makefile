PACKAGE := megfile
VERSION := $(shell cat ${PACKAGE}/version.py | sed -n -E 's/.*=//; s/ //g; s/"//g; p')

test:
	pytest --cov-config=setup.cfg --cov=${PACKAGE} --disable-socket --no-cov-on-fail --cov-report=html:html_cov/ --cov-report term-missing --cov-report=xml tests/ --durations=10

format:
	isort ${PACKAGE} tests
	yapf --in-place --recursive ${PACKAGE} tests

style_check:
	isort --diff --check ${PACKAGE} tests
	yapf --diff --recursive ${PACKAGE} tests

static_check:
	make pytype_check

pytype_check:
	pytype

bandit_check:
	bandit --format sarif -r megfile/ > bandit-sarif.json

pyre_check:
	pyre --output=json check > pyre-errors.json || echo
	cat pyre-errors.json | ./scripts/convert_results_to_sarif.py > pyre-sarif.json

mut:
	@echo Mutation testing...
	mutmut run || echo
	mutmut show all
	mutmut junitxml > mutmut.xml

doc:
	python3 setup.py build_sphinx --fresh-env --build-dir html_doc/

release:
	git tag ${VERSION}
	git push origin ${VERSION}

	rm -rf build dist
	python3 setup.py bdist_wheel

	devpi login ${PYPI_USERNAME} --password=${PYPI_PASSWORD}
	devpi upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl

	twine upload dist/${PACKAGE}-${VERSION}-py3-none-any.whl --username=${PYPI_USERNAME_2} --password=${PYPI_PASSWORD_2}
