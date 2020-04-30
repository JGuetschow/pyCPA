#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 21:58:30 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""

# import submodules from subfolder
from . import core
from . import readData
from . import tools

__all__ = ['core', 'readData', 'tools']


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
