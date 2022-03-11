#!/usr/bin/env python

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
    setup_requires=['pbr>=5.8.1'],
    scripts=[ccmscript],
    pbr=True,
)
