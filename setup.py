#!/usr/bin/env python

"""
Setup script for scops

"""

import glob
from distutils.core import setup

scripts_list = glob.glob('scops*.py')

setup(name='SCOPS',
      version = '1.0',
      description = 'The Simple Concurrent Online Processing System (SCOPS)',
      url = 'https://nerc-arf-dan.pml.ac.uk',
      packages = ['scops'],
      scripts = scripts_list,)
