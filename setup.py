#!/usr/bin/env python

from shutil import copyfile

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

copyfile('ccm', 'ccm.py')

setup(
    setup_requires=['pbr>=5.8.1'],
    pbr=True,
)
