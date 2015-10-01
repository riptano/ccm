#!/usr/bin/env python

from os.path import abspath, join, dirname
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

setup(
    name='ccm',
    version='2.0.5',
    description='Cassandra Cluster Manager',
    long_description=open(abspath(join(dirname(__file__), 'README.md'))).read(),
    author='Sylvain Lebresne',
    author_email='sylvain@datastax.com',
    url='https://github.com/pcmanus/ccm',
    packages=['ccmlib', 'ccmlib.cmds'],
    scripts=[ccmscript],
    install_requires=['pyYaml', 'six >=1.4.1'],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],

)
