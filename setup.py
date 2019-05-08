#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

setup_requirements = ['pytest-runner', ]

test_requirements = []
with open("requirements_dev.txt") as dev_requirements:
    for line in dev_requirements:
        line = line.strip()
        if len(line) == 0:
            continue
        if line[0] == '#':
            continue

        version_pin = line.split()[0]
        test_requirements.append(version_pin)

install_requires = []
with open("requirements.txt") as requirements:
    for line in requirements:
        line = line.strip()
        if len(line) == 0:
            continue
        if line[0] == '#':
            continue

        version_pin = line.split()[0]
        install_requires.append(version_pin)

data_files = [('api', ['rnaget_service/api/rnaget.yaml']),
              ('data', ['rnaget_service/expression/feature_mapping_HGNC.tsv'])]

setup(
    author="BCGSC",
    author_email='alipski@bcgsc.ca',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="A POC implementation of GA4GH RNAGET API",
    install_requires=install_requires,
    license="GNU General Public License v3",
    include_package_data=True,
    keywords='rnaget_service',
    name='rnaget_service',
    packages=find_packages(include=['rnaget_service']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    data_files=data_files,
    url='https://github.com/CanDIG/rnaget_service',
    version='0.9.1',
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'rnaget_service = rnaget_service.app:main',
            'rnaget_add_expression = rnaget_service.scripts.post_data:add_expression_file',
            'rnaget_add_project = rnaget_service.scripts.post_project:add_project',
            'rnaget_add_study = rnaget_service.scripts.post_study:add_study'
            ]
        },
)
