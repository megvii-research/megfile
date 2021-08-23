import os
import sys

from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand
from sphinx.setup_command import BuildDoc
from importlib.machinery import SourceFileLoader

requirements = []
with open('requirements.txt') as f:
    requirements = f.readlines()

test_requirements = []
with open('requirements-dev.txt') as f:
    test_requirements = f.readlines()


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to pytest')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = 'tests/'

    def run_tests(self):
        import shlex

        # import here, cause outside the eggs aren't loaded
        import pytest

        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)


def load_version(filename):
    loader = SourceFileLoader(filename, filename)
    return loader.load_module().VERSION

setup(
    name='megfile',
    description='R-eng team file operation library',
    version=load_version('megfile/version.py'),
    author='r-eng',
    author_email='r-eng@megvii.com',
    url='https://github.com/megvii-research/megfile',
    packages=find_packages(exclude=('tests', 'tests*', 'remof')),
    scripts=[entry.path for entry in os.scandir('bin') if entry.is_file()],
    classifiers=[
        'License :: Other/Proprietary License',
    ],
    cmdclass={
        'test': PyTest,
        'build_sphinx': BuildDoc,
    },
    tests_require=test_requirements,
    install_requires=requirements,
    python_requires='>=3.5',
)
