#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 22:12:56 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""

import scmdata
import os
import pandas as pd

def read_CRF_data(file, folder, col_rename, col_defaults) -> scmdata.dataframe.ScmDataFrame:
    
    CRF_data = pd.DataFrame
    CRF_data = pd.read_csv(os.path.join(folder, file))
        
    ## rename headers 
    ## include checks and throw errors
    columns = CRF_data.columns.values 
    for col in col_rename:
        columns[columns == col] = col_rename[col]

    CRF_data.columns = columns
    
    ## add default value columns
    ## TODO include checks if cols already present and throw errors
    for col in col_defaults:
        CRF_data.insert(0, col, col_defaults[col])
    
    ## if class column exists, rename it because that is a python keyword
    ## if it doesn't exist initialize with 'TOTAL'
    if 'class' in CRF_data.columns.values:
        CRF_data.columns.values[CRF_data.columns.get_loc("class")] = "class_t"
    else:
        idx_variable_col = CRF_data.columns.get_loc("variable")
        CRF_data.insert(idx_variable_col + 1, 'class_t', 'TOTAL')
        
    
    ## convert to scmdata dataframe
    CRF_scm = scmdata.dataframe.ScmDataFrame(CRF_data)
    
    ## return
    return CRF_scm