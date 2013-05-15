#!/usr/bin/python

from distutils.core import setup
from os.path import abspath, join, dirname

setup(
    name='ccm',
    version='1.0dev',
    description='Cassandra Cluster Manager',
    long_description=open(abspath(join(dirname(__file__), 'README'))).read(),
    author='Sylvain Lebresne',
    author_email='sylvain@datastax.com',
    url='https://github.com/pcmanus/ccm',
    packages=['ccmlib', 'ccmlib.cmds'],
    scripts=['ccm'],
    requires=['pyYaml'],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
    ],

)
