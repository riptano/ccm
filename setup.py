#!/usr/bin/env python

from os.path import abspath, dirname, join
from platform import system
from shutil import copyfile

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

ccmscript = 'ccm'
if system() == "Windows":
    copyfile('ccm', 'ccm.py')
    ccmscript = 'ccm.py'

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='ccm',
    version='3.1.6',
    description='Cassandra Cluster Manager',
    author='Sylvain Lebresne',
    author_email='sylvain@datastax.com',
    url='https://github.com/riptano/ccm',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=['ccmlib', 'ccmlib.cmds'],
    scripts=[ccmscript],
    install_requires=['pyYaml', 'six >=1.4.1', 'psutil'],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],

)
