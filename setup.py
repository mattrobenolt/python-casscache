#!/usr/bin/env python
"""
python-casscache
~~~~~~~~~~~~~~~~

Casscache is a python-memcached compatible API for interfacing
with Cassandra.

:copyright: (c) 2013 by Matt Robenolt, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""

from setuptools import setup

setup(
    name='casscache',
    version='0.1.1',
    author='Matt Robenolt',
    author_email='matt@ydekproductions.com',
    url='https://github.com/mattrobenolt/python-casscache',
    description='Casscache is a python-memcached compatible API for interfacing with Cassandra',
    license='BSD',
    long_description=__doc__,
    install_requires=[
        'cassandra-driver'
    ],
    py_modules=['casscache'],
    test_suite='test_casscache',
    zip_safe=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
