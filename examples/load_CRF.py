#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:07:33 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de

This script can be run from any location given that pyCPA is installed as a python module

Input data has to be made available in the subfolder specified by <data_folder>

"""

import os
import pyCPA

# configuration
data_folder = 'data'
specification_folder = 'metadata/category_conversion'
mapping_file = 'conversion_IPCC2006-1996_pyCPA_example.csv'
CRF_file = 'Guetschow-et-al-2019-PRIMAP-crf_2019-v2.csv'
split_name = CRF_file.split('.csv')
converted_CRF_file = split_name[0] + '_pyCPA.csv'
IPCC1996_CRF_file = split_name[0] + '_pyCPA_IPCC1996.csv'

## CRF reading define which columns should be renamed 
col_replacements = {
    'country': 'region',
    'entity': 'variable',
    'class': 'classification'
    }

col_defaults = {
    'model': 'UNFCCC-CRF'
    }

## filtering of input data (all rows with other metadata values for the given columns are discarded)
col_filter = {
    'variable': ['CO2', 'CH4', 'N2O', 'SF6', 'HFCSAR4', 'PFCSAR4', 'NF3'],
    'classification': ['TOTAL']
    }
defaultGWP = 'AR4'


## definitions necessary for the mapping
conversion = {
    'from_code_mapping': 'CODE2006',
    'column_data': 'category', #tell it that it's working on the category column
    'to_code_mapping': 'CODE1996',
}

## additional information from the mapping table that should be stored in the resulting dataframe
additional_fields = {
    'FULLNAME1996': "categoryDescription" #additionally create a column with the category name
}

# remove columns before the category conversion (here none removed)
cols_to_remove = []

# order for the metadata columns in output data
meta_cols = ['model', 'scenario', 'region', 'category', 'categoryDescription', 'classification', 
             'variable', 'unit', 'unit_context']
meta_cols_no_cat_desc = ['model', 'scenario', 'region', 'category', 'classification', 
             'variable', 'unit', 'unit_context']

### read the CRF data and prepare for conversion ###
# read the data including setting of default values and replacing column names to match the 
# pyCPA column names 
CRF_scm = pyCPA.readData.read_wide_csv_file(CRF_file, data_folder, col_replacements, col_defaults)

# filter the dataframe according to configuration
CRF_scm.filter(inplace = True, **col_filter)

# add GWP information based on variable names and default  GWP given above
pyCPA.core.add_GWP_information(CRF_scm, defaultGWP)

# convert units such that thewy match the pyCPA definitions (which are given by the scmdata package used internally) 
pyCPA.core.convert_unit_PRIMAP_to_scmdata(CRF_scm)

# convert category codes from PRIMAP to pyCPA format 
pyCPA.core.convert_IPCC_categories_PRIMAP_to_pyCPA(CRF_scm)

# convert to IPCC1996 (just a few subcategories)
CRF_in_IPCC1996 = pyCPA.tools.conversion.map_data(CRF_scm, mapping_file, specification_folder, conversion, 
                                                  add_fields = additional_fields, 
                                                  cols_to_remove = cols_to_remove, verbose = False)

# save csv IPCC2006 categories
CRF_scm_ts = CRF_scm.timeseries().reorder_levels(meta_cols_no_cat_desc)
CRF_scm_ts.to_csv(os.path.join(data_folder, converted_CRF_file), date_format = 'YYYY')

# save csv IPCC1996 categories
CRF_in_IPCC1996_ts = CRF_in_IPCC1996.timeseries().reorder_levels(meta_cols)
CRF_in_IPCC1996_ts.to_csv(os.path.join(data_folder, IPCC1996_CRF_file), date_format = 'YYYY')

