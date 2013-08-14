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
    version='0.0.0',
    author='Matt Robenolt',
    author_email='matt@ydekproductions.com',
    url='https://github.com/mattrobenolt/python-casscache',
    description='Casscache is a python-memcached compatible API for interfacing with Cassandra',
    license='BSD',
    long_description=__doc__,
    install_requires=[
        'cassandra==0.1.4'
    ],
    dependency_links=[
        'https://github.com/mattrobenolt/python-driver/archive/4faa2954a1027fd8f92bb5efa86e27d21291c2e6.zip#egg=cassandra-0.1.4',
    ],
    py_modules=['casscache'],
    test_suite='test_casscache',
    zip_safe=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Topic :: Software Development'
    ],
)
