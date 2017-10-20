#!/usr/bin/env python

import versioneer
from setuptools import setup
from os.path import exists


setup(name='aiopeewee',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      packages=['aiopeewee'],
      description='Async Peewee',
      url='http://github.com/kszucs/aiopeewee',
      maintainer='Krisztian Szucs',
      maintainer_email='szucs.krisztian@gmail.com',
      license='MIT',
      keywords='async asyncio peewee orm',
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
      ],
      install_requires=['peewee', 'aiomysql'],
      tests_require=['pytest-asyncio', 'pytest'],
      setup_requires=['pytest-runner'],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      zip_safe=False)
