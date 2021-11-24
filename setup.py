"""The neo4japanese setup.py script."""

from setuptools import setup, find_packages


setup(
    name='neo4japanese',
    version='0.1.0',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        'lxml',
        'neo4j',
        'pydantic',
    ],
)
