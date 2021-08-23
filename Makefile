PACKAGENAME := megfile
VERSION := $(shell cat ${PACKAGENAME}/version.py | sed -n -E 's/^VERSION = "(.+?)"/\1/p')

test:
	pytest --cov-config=setup.cfg --cov=${PACKAGENAME} --disable-socket --no-cov-on-fail --cov-report=html:html_cov/ --cov-report term-missing tests/

format:
	isort ${PACKAGENAME} tests
	yapf --in-place --recursive ${PACKAGENAME} tests

style_check:
	isort --diff --check ${PACKAGENAME} tests
	yapf --diff --recursive ${PACKAGENAME} tests

static_check:
	pytype

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
	devpi upload dist/${PACKAGENAME}-${VERSION}-py3-none-any.whl
