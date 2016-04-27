import sys
import re
from setuptools import setup, find_packages


def get_version():
    with open('traptor/version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')


def readme():
    ''' Returns README.rst contents as str '''
    with open('README.rst') as f:
        return f.read()

# with open('requirements.txt') as f:
#     install_requires = [x.strip() for x in f]
install_requires = [
    'birdy>=0.2',
    'requests>=1.2.3',
    'requests-oauthlib>=0.3.2',
    'redis>=2.10.3',
    'kafka-python>=0.9.5',
    'python-dateutil',
    'click',
    'mock',
    'scutils>=0.0.6',
    'flatdict',
    'raven'
]

lint_requires = [
    'pep8',
    'pyflakes'
]

tests_require = [
    'mock',
    'pytest',
    'pymysql',
    'pytest-cov',
    'pytest-xdist'
]
dependency_links = []
setup_requires = ['pytest-runner']
extras_require = {
    'test': tests_require,
    'all': install_requires + tests_require,
    'docs': ['sphinx'] + tests_require,
    'lint': lint_requires
}

setup(
    name='traptor',
    version=get_version(),
    description='A distributed twitter streaming service',
    long_description=readme(),
    author='Jason Haas',
    author_email='jasonrhaas@gmail.com',
    license='MIT',
    url='https://github.com/istresearch/traptor',
    keywords=['twitter', 'distributed', 'kafka', 'ansible', 'redis'],
    packages=['traptor'],
    package_data={},
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    extras_require=extras_require,
    dependency_links=dependency_links,
    zip_safe=True,
    include_package_data=True,
)
