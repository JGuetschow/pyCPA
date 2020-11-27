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


# config
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
    'entity': 'variable'
    }

col_defaults = {
    'model': 'CRF2019'
    }

## filtering of input data (all rows with other metadata values for the given columns are discarded)
col_filter = {
    'variable': ['CO2', 'CH4', 'N2O', 'SF6', 'HFCSAR4', 'PFCSAR4', 'NF3'],
    'class_t': ['TOTAL']
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
    "Category Name": 'FULLNAME1996' #additionally create a column with the category name
}

cols_to_remove = ['class_t']

# read the CRF data 
CRF_scm = pyCPA.readData.read_wide_csv_data(CRF_file, data_folder, col_replacements, col_defaults)

# filter the dataframe according to configuration
CRF_scm.filter(inplace = True, **col_filter)

# TODO: integrate the following in the reading procedure?
# add GWP information
pyCPA.core.add_GWP_information(CRF_scm, defaultGWP)

# convert units
pyCPA.core.convert_unit_PRIMAP_to_scmdata(CRF_scm)

# convert category codes
pyCPA.core.convert_IPCC_categories_PRIMAP_to_pyCPA(CRF_scm)

# convert to IPCC1996 (just a few subcategories)
CRF_in_IPCC1996 = pyCPA.tools.conversion.map_data(CRF_scm, mapping_file, specification_folder, conversion, additional_fields, 
                      cols_to_remove = cols_to_remove, verbose = False)

# save csv IPCC2006 categories
CRF_scm.to_csv(os.path.join(data_folder, converted_CRF_file), date_format = 'YYYY')

# save csv IPCC1996 categories
CRF_in_IPCC1996.to_csv(os.path.join(data_folder, IPCC1996_CRF_file), date_format = 'YYYY')

