#!/usr/bin/env python

import astroplant_kit
from setuptools import setup, find_packages


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name='astroplant-kit',
      version=astroplant_kit.__version__,
      description='AstroPlant kit',
      author='Thomas Churchman',
      author_email='thomas@kepow.org',
      url='https://astroplant.io',
      scripts=['scripts/astroplant-kit'],
      packages=find_packages(),
      package_data={'astroplant_kit': ['proto/*',],},
      # Require for capnp, see https://github.com/capnproto/pycapnp/issues/72
      zip_safe=False,
      install_requires=requirements,
     )
