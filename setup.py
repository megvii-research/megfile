from setuptools import find_packages, setup
from sphinx.setup_command import BuildDoc
from importlib.machinery import SourceFileLoader


def load_version(filename):
    loader = SourceFileLoader(filename, filename)
    return loader.load_module().VERSION


def load_text(filename):
    with open(filename) as fd:
        return fd.read()


def load_requirements(filename):
    return load_text(filename).splitlines()


requirements = load_requirements("requirements.txt")
test_requirements = load_requirements("requirements-dev.txt")

setup(
    name='megfile',
    description='Megvii file operation library',
    long_description=load_text('README.md'),
    long_description_content_type='text/markdown',
    version=load_version('megfile/version.py'),
    author='megvii',
    author_email='megfile@megvii.com',
    url='https://github.com/megvii-research/megfile',
    packages=find_packages(exclude=('tests', 'tests*', 'remof')),
    entry_points={'console_scripts': ['megfile = megfile.cli:safe_cli']},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    cmdclass={
        'build_sphinx': BuildDoc,
    },
    tests_require=test_requirements,
    install_requires=requirements,
    python_requires='>=3.6',
)
