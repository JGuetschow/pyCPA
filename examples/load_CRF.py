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
# TODO: specification_folder = 'metadata/category_conversion'
# TODO: mapping_file = 'IPCC2006_2_IPCC1996.csv'
CRF_file = 'Guetschow-et-al-2019-PRIMAP-crf_2019-v2.csv'
split_name = CRF_file.split('.csv')
converted_CRF_file = split_name[0] + '_pyCPA.csv'

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
    'from_code_mapping': 'IPCC code',
    'column_data': 'category',
    'to_code_mapping': 'Code',
}

## additional information from the mapping table that should be stored in the resulting dataframe
additional_fields = {
    'Description': "Category Description",
    'Type': "type_t"
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

# TODO: convert to IPCC1996
# CRF_in_IPCC1996 = pyCPA.tools.conversion.map_data(CRF_scm, mapping_file, specification_folder, conversion, additional_fields, 
 #                      cols_to_remove = cols_to_remove, verbose = False)

# remove class_t column as not needed here

# save csv
CRF_scm.to_csv(os.path.join(data_folder, converted_CRF_file), date_format = 'YYYY')


