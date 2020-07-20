#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 21:57:09 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""

# import function from files to make them available without a further nesting level
from .read_data import read_wide_csv_data

__all__ = ['read_wide_csv_data']