# -*- coding: iso-8859-1 -*-
import os
import sys

from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

if sys.version_info[:2] < (2, 6):
    print 'stompest requires Python version 2.6 or later (%s detected).' % '.'.join(sys.version_info[:2])
    sys.exit(-1)

setup(
    name = 'stompest',
    version = '2.0a1',
    author = 'Jan MŸüller',
    author_email = 'nikipore@gmail.com',
    description = 'STOMP client library for Python including both synchronous and Twisted clients.',
    license = 'Apache License 2.0',
    packages = find_packages(),
    long_description=read('README.txt'),
    keywords = 'stomp twisted activemq rabbitmq apollo',
    url = 'https://github.com/nikipore/stompest',
    py_modules = ['stompest'],
    include_package_data = True,
    zip_safe = True,   
    install_requires = [
        'twisted'
    ],
    tests_require = ['mock'],
    test_suite = 'stompest.tests',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Twisted',
        'Topic :: System :: Networking',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
