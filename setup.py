#!/usr/bin/env python
from setuptools import setup, find_packages
from imp import load_source
from os import path
import io

__version__ = load_source('ml_tm_utils_pub.version', 'ml_tm_utils_pub/version.py').__version__

here = path.abspath(path.dirname(__file__))

# get the dependencies and installs
with io.open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if 'git+' not in x]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' not in x]

setup(
    name='ml_tm_utils_pub',
    author='',
    author_email='',
    version=__version__,
    description='ML utilities for the Humanitarian OpenStreetMap Team\'s Tasking Manager',
    url='https://github.com/developmentseed/ml_tm_utils_pub',
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='',
    entry_points={
    },
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    install_requires=install_requires,
    dependency_links=dependency_links,
)
