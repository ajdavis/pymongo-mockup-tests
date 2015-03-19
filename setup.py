#!/usr/bin/env python

# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

pymongo_link = (
    'https://github.com/mongodb/mongo-python-driver/archive/3.0b0.tar.gz'
    '#egg=pymongo')

mockupdb_link = 'git+git://github.com/ajdavis/mongo-mockup-db.git'

setup(
    name='PyMongo MockupDB tests',
    version='0.1.0',
    description="Test PyMongo with MockupDB.",
    long_description=readme,
    author="A. Jesse Jiryu Davis",
    author_email='jesse@mongodb.com',
    url='https://github.com/ajdavis/pymongo-mockup-tests',
    tests_require=['pymongo', 'mockupdb'],
    dependency_links=[pymongo_link, mockupdb_link],
    license="Apache License, Version 2.0",
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        "License :: OSI Approved :: Apache Software License",
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests')
