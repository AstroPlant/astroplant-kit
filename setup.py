#!/usr/bin/env python

import astroplant_kit
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(name='astroplant-kit',
      version=astroplant_kit.__version__,
      description='AstroPlant kit',
      author='Thomas Churchman',
      author_email='thomas@kepow.org',
      url='https://astroplant.io',
      scripts=['scripts/astroplant-kit'],
      packages=['astroplant_kit',],
      install_requires=requirements,
     )
