#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 10:32:08 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""


import pandas as pd
import scmdata
import os
from ..src.pyCPA.tools.data_analysis import check_coverage



data_folder = 'test_data'
#data_file = 'test_conv.csv'
data_file = 'Guetschow-et-al-2019-PRIMAP-crf_2019-v2_NACE.csv'
#data_file = 'PRIMAPCRF_2019V2B1_25-Mar-2020_NACE.csv'

#converted_CRF_file = 'test_conv.csv'

# read the data 
input_data = pd.DataFrame
input_data = pd.read_csv(os.path.join(data_folder, data_file))
## convert to scmdata dataframe
data_scm = scmdata.dataframe.ScmDataFrame(input_data)
## analyze coverage
cov_filter = {'type_t': 'total'}

coverage = check_coverage(data_scm, cov_filter, ['variable', 'category'])




#CRF_scm.to_csv(os.path.join(data_folder, converted_CRF_file))
