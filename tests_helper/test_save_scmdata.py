#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 09:15:35 2020

@author: johannes
"""

import pandas as pd
import scmdata
import os

data_folder = '../test_data'
CRF_file = 'test_data.csv'
converted_CRF_file = 'test_conv.csv'

# read the CRF data 
CRF_data = pd.DataFrame
CRF_data = pd.read_csv(os.path.join(data_folder, CRF_file))
## convert to scmdata dataframe
CRF_scm = scmdata.dataframe.ScmDataFrame(CRF_data)
## save csv
CRF_scm.to_csv(os.path.join(data_folder, converted_CRF_file))
