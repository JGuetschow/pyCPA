#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 21:42:01 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""

# import function from files to make them available without a further nesting level
# TODO: it might make sense to introduce a structure here as tools will be a large module
from . import conversion
from . import dataAnalysis

__all__ = ['conversion', 'dataAnalysis']
