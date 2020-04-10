#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:17:57 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""

import scmdata
import pandas as pd
import os
from pyCPA.core import combine_rows

def map_data(input_DF, mapping_file, folder, conversion, add_fields, cols_to_remove: list = [], 
             verbose: bool = False) -> scmdata.dataframe.ScmDataFrame:
    """
    Map data from one metadata column to another one including summing and subtraction
    
    Parameters
    ----------
    
    input\_DF
        ScmDataFrame with data to be mapped
    mapping\_file
        csv file that contains the mapping rules
    folder
        the folder the mapping file resides in
    conversion
        a dict defining the rows to use in the input\_DF the mapping\_file and the output DF
    add\_fields
        a dict defining metadata columns to add and their value
    cols\_to\_remove
        a list defining columns that will be removed: default is an empty list
    verbose
        bool: if set to true a lot of debug output will be written to the terminal
    
    Returns
    -------
    
    :obj:`scmdata.dataframe.ScmDataFrame`
        scmDataFrame with the converted data
        
    """
    
    # read the mapping table
    mapping_table = pd.DataFrame
    mapping_table = pd.read_csv(os.path.join(folder, mapping_file))
    
    # loop over entires of mapping table
    for iCode in range(0, len(mapping_table)):
        # get to_code
        to_code_current = mapping_table[conversion['to_code_mapping']].iloc[iCode]
        
        if verbose:
            print('#########################')
            print('Working on category ' + to_code_current + ' (' + str(iCode) + ')')
            print('#########################')
        # get from_code from mapping table and analyze it
        from_code_current = mapping_table[conversion['from_code_mapping']].iloc[iCode]
        # check if empty (then it's not a string, but a float with content nan)
        if isinstance(from_code_current, str):
            parts = from_code_current.split(' ')
            operator = ['+'] + parts[1::2]
            categories = parts[0::2]
                
            # prepare and make a call to map the data and combine if necessary
            converted_data = combine_rows(input_DF, conversion['column_data'], categories, to_code_current, {}, 
                                          operator, inplace = False, cols_to_remove = cols_to_remove, 
                                          verbose = verbose)
            if converted_data is not None:
                # add additional columns
                if verbose:
                    print('Adding data for sector ' + to_code_current)
                for key in add_fields:
                    converted_data.set_meta(mapping_table[key].iloc[iCode], add_fields[key])
            
                # add the data to converted DF
                if 'DF_mapped' in locals():
                    DF_mapped.append(converted_data, inplace = True)
                else:
                    DF_mapped = converted_data.copy()
            else:
                if verbose:
                    print('No input data for sector ' + to_code_current)     
        else:
            if verbose:
                print('No source category for target category ' + to_code_current + ' given.')
        
    return DF_mapped