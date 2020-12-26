#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:17:57 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
.. highlight:: python
"""

import scmdata
import pandas as pd
import os
import re
from pyCPA.core import combine_rows



def map_data(input_DF, mapping_file, folder, conversion, onlynew: bool = True, add_fields: dict = {}, 
             cols_to_remove: list = [], verbose: bool = False) -> scmdata.run.ScmRun:
    """
    Map data from one metadata column to another one including summing and subtraction. This function can be used e.g. 
    to map data from one categorization into another one, or for data preprocessing.
    
    Parameters
    ----------
    
    input\_DF : scmdata.run.Scmrun
        ScmDataFrame with data to be mapped
    
    mapping\_file : str
        csv file that contains the mapping rules.
        
    folder : str
        the folder the mapping file resides in
        
    conversion : dict
        a dict defining the rows to use in the input\_DF the mapping\_file and the output DF. there are two 
        forms for the dict lined out on the examples section
        
    onlynew : bool
        bool. If True only aggregated data will be returned. Else aggregated data added 
        will be added to the input DF and the resulting DF will be returned. Default True
    
    add\_fields : dict
        a dict defining metadata columns to add and the column in the conversion file 
        defining their value: default is an empty dict
        Do not use with the generalized conversion format because the information can 
        be integrated into the conversion
                    
    cols\_to\_remove : list
        a list defining columns that will be removed: default is an empty list
    
    verbose : bool
        bool: if set to true a lot of debug output will be written to the terminal
    
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        scmRun with the converted data
        
    Examples
    --------
    
    conversion dict complex form::
        
        conversion = {
            "category": ["CRFcategory", "IPCCcategory"], 
            "categoryName": ["*", "IPCCcategoryName"], 
            "classification": ["CRFclass", "IPCCclass"]
        }
        
    conversion dict simplified form::
        
        conversion = {
            'from_code_mapping': 'categoryCode',
            'column_data': 'category', #tell it that it's working on the category column
            'to_code_mapping': 'CODE1996',
        }
    
    add\_fields example::
        
        add_fields = {"type_t": "type"}
    
    
    """
    
    
    
    # check if parameters make sense
    if add_fields and not onlynew:
        raise ValueError("Adding fields is only possible with onlynew = True") 
    
    sorted_keys = sorted(list(conversion.keys()))
    if sorted_keys == ["column_data", "from_code_mapping", "to_code_mapping"]:
        conversion = {
            conversion["column_data"]: [conversion["from_code_mapping"], conversion["to_code_mapping"]]
        }
    
    allowed_operators = ['+', '-']
    
    # read the mapping table
    mapping_table = pd.read_csv(os.path.join(folder, mapping_file))
    
    first_data = True

    # loop over entires of mapping table
    for iRow in range(0, len(mapping_table)):
        if verbose:
            print('#########################')
            print('Working on row ' + str(iRow))
            print(mapping_table.iloc[iRow]) 
            print('#########################')
        
        
        columns = conversion.keys()
        
        combo = {}
        skip = False
        for column in columns:
            # for each column create the entry in the combination dict
            # first get input values
            if conversion[column][0] == '*':
                input_values = ['*']
                operator = '+'
            else:
                # get from_code from mapping table and analyze it
                from_value_current = mapping_table[conversion[column][0]].iloc[iRow]
                # convert to str if it's not
                if not isinstance(from_value_current, str):
                    # convert to str
                    from_value_current = str(from_value_current)

                if from_value_current not in ['None', '', 'nan']:
                    input_values = re.split('\s[\+-]\s', from_value_current)
                    operator = re.findall('(?<=\s)[\+-](?=\s)', from_value_current)
                    first_op = re.findall('^[\+-](?=\s)', input_values[0])
                    if first_op:
                        operator = first_op + operator
                        input_values[0] = input_values[0][2:]
                    else:
                        operator = ['+'] + operator

                    # check if operator only consists of allowed operators
                    if not all(op in allowed_operators for op in operator):
                        print('Illegal operator found for column ' + column + ' in ' + from_value_current + ', row {:d}'.format(iRow))
                        break

            # get the output value
            to_value_current = mapping_table[conversion[column][1]].iloc[iRow]
            # convert to str if it's not
            if not isinstance(to_value_current, str):
                # convert to str
                to_value_current = str(to_value_current)

            # check if conversion for column makes sense
            if from_value_current in ['\\NOTCOVERED', '\\ZERO']:
                skip = True
                break
            if to_value_current in ['None', '', 'nan']:
                if from_value_current in ['None', '', 'nan']:
                    # from also empty, then ignore column
                    skip = True
                    if verbose:
                        print('Conversion for column ' + column + ' empty for row {:d}'.format(iRow))
                        print('to_value: ' + to_value_current + ', from_value: ' + from_value_current)
                else:
                    # to is empty, from not. That is an error in the mapping table
                    skip = True
                    print('Inconsistent mapping table for ' + column + ', row {:d}'.format(iRow))
                    print('to_value: ' + to_value_current + ', from_value: ' + from_value_current)
                    break
            else:
                if from_value_current in ['None', '','nan']:
                    # from is empty, to not. That is an error in the mapping table
                    skip = True
                    print('Inconsistent mapping table for ' + column + ', row {:d}'.format(iRow))
                    print('to_value: ' + to_value_current + ', from_value: ' + from_value_current)
                    break
                else:
                    combo[column] = [input_values, operator, to_value_current]
        
        # check if the resulting cobination dict is not empty
        if not combo and not skip:
            print('Conversion dict is empty for row {:d}'.format(iRow))
        elif not skip:
            # prepare and make a call to map the data and combine if necessary
            converted_data = combine_rows(input_DF, combo, {}, inplace = False, 
                                          cols_to_remove = cols_to_remove, verbose = verbose)
            
            if converted_data is not None:
                # add additional columns
                if verbose:
                    print('Adding data for row {:d}'.format(iRow) + '. combo dict:')
                    print(combo)
                    
                for key in add_fields:
                    #converted_data.set_meta(mapping_table[key].iloc[iRow], add_fields[key])
                    if verbose:
                        print('cols before insertion')
                        print(converted_data.meta.columns.values)
                    converted_data.__setitem__(add_fields[key], mapping_table[key].iloc[iRow])
                    if verbose:
                        print('cols after insertion')
                        print(converted_data.meta.columns.values)
                    
                # add the data to converted DF
                if first_data:
                    first_data = False
                    if onlynew:
                        DF_mapped = converted_data.copy()
                    else:
                        DF_mapped = input_DF.copy()
                        # remove the columns
                        DF_mapped.drop_meta(cols_to_remove, inplace = True)
                        DF_mapped.append(converted_data, inplace = True)                        
                else:
                    if verbose:
                        print('cols before adding')
                        print(converted_data.meta.columns.values)
                    
                    DF_mapped.append(converted_data, inplace = True)
            else:
                if verbose:
                    print('No input data for row {:d}'.format(iRow) + '.combo dict:')
                    print(combo)     
        
    return DF_mapped