#!/usr/bin/env python

from setuptools import setup
from os.path import exists


setup(name='aiopeewee',
      version='0.2.0',
      packages=['aiopeewee'],
      description='Async Peewee',
      url='http://github.com/kszucs/pandahouse',
      maintainer='Krisztian Szucs',
      maintainer_email='szucs.krisztian@gmail.com',
      license='BSD',
      keywords='',
      install_requires=['peewee', 'aiomysql'],
      tests_require=['pytest-aiohttp', 'pytest'],
      setup_requires=['pytest-runner'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      zip_safe=False)
