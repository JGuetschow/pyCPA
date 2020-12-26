#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 22:12:56 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
.. highlight:: python
"""

import scmdata
import os
import pandas as pd
import re
import yaml


def read_wide_csv_file(file, folder, col_rename: dict = {}, col_defaults: dict = {}, 
                       meta_mapping: dict = {}, filter_keep: dict = {}, 
                       filter_remove: dict = {}) -> scmdata.run.ScmRun:
    """
    This function reads a csv file and returns the data as an ScmDataFrame 
    
    Columns can be renamed or filled with default vales to match the pyCPA structure.
    The individual files are read using "read_wide_csv_data"
    
    Currently duplicate datapoint will not be detected
    
    Parameters
    ----------
    
    file : str
        String containing the file name    
    
    folder : str
        String with folder name to read from
    
    col\_rename : dict
        Dict where the keys are column names in the files to be read and the value is the column file 
        in the output dataframe. Use '\DROP' as value to remove a column. Default = empty
    
    col\_defaults : dict
        Dict for default values of columns not given in the csv files. The keys are the column names and 
        the values are the values for the columns. Default: empty
    
    meta\_mapping : dict
        A dict with pyCPA column names as keys. Values are dicts with input values as 
        keys and output values as values. A standard use case is to map gas names from 
        input data to the standardized names used in pyCPA / scmdata. Default: empty
            
    filter\_keep : dict
        Dict defining filters of data to keep. Filtering is done after metadata mapping, 
        so use mapped metadata values to define the filter. Column names are pyCPA columns.
        Each entry in the dict defines an individual filter. The names of the filters have no 
        relevance. Default: empty
            
    filter\_remove : dict
        Dict defining filters of data to remove. Filtering is done after metadata mapping, 
        so use mapped metadata values to define the filter. Column names are pyCPA columns.
        Each entry in the dict defines an individual filter. The names of the filters have no 
        relevance. Default: empty
        
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        ScmRun with the read data
        
        
    Examples
    --------
    
    example for meta\_mapping::
    
        meta_mapping = {
            'pyCPA_col_1': {'col_1_value_1_in': 'col_1_value_1_out',
                            'col_1_value_2_in': 'col_1_value_2_out',
                            },
            'pyCPA_col_2': {'col_2_value_1_in': 'col_2_value_1_out',
                            'col_2_value_2_in': 'col_2_value_2_out',
                            },
        }
    
    example for filter\_keep::
        
            filter_keep = {
                'f_1': {'variable': ['CO2', 'CH4'], 'region': 'USA'},
                'f_2': {'variable': 'N2O'}        
            } # this example filter keeps all CO2 and CH4 data for the USA and N2O data for all countries
    
    example for filter\_remove::
        
        filter_remove = {
            'f_1': {'scenario': 'HISTORY'},
        } # this filter removes all data with 'HISTORY' as scenario
        
    """
    
    #read_data = pd.DataFrame
    read_data = pd.read_csv(os.path.join(folder, file))
        
    ## rename headers 
    ## include checks and throw errors
    columns = read_data.columns.values 
    for col in col_rename:
        if col_rename[col] == '\DROP':
            read_data.drop(columns = [col], inplace = True)
    
    # columns may have changed!
    columns = read_data.columns.values
    for col in col_rename:
        if col_rename[col] != '\DROP':
            columns[columns == col] = col_rename[col]

    ## add default value columns
    ## TODO include checks if cols already present and throw errors
    for col in col_defaults:
        read_data.insert(0, col, col_defaults[col])
    
    ## if class column exists, rename it because that is a python keyword
    if 'class' in read_data.columns.values:
        read_data.columns.values[read_data.columns.get_loc("class")] = "classification"
    
    ## if classification doesn't exist initialize with 'TOTAL'
    if not 'classification' in read_data.columns.values:
        idx_variable_col = read_data.columns.get_loc("variable")
        read_data.insert(idx_variable_col + 1, 'classification', 'TOTAL')
        
    ## replace strings with NaN in year columns
    year_regex = re.compile('\d')
    year_cols = list(filter(year_regex.match, read_data.columns.values))
    
    ## remove all non-numeric values from year columns
    for col in year_cols:
        read_data[col] = read_data[col][pd.to_numeric(read_data[col], errors='coerce').notnull()]
    
    # metadata mapping
    if meta_mapping:
        read_data.replace(meta_mapping, inplace = True)
    
    # convert to ScmRum
    read_data_scm = scmdata.run.ScmRun(read_data)
    
    # filter the data
    for column in filter_keep.keys():
        filter_current = filter_keep[column]
        read_data_scm.filter(inplace = True, keep = True, **filter_current)
    
    for column in filter_remove.keys():
        filter_current = filter_remove[column]
        read_data_scm.filter(inplace = True, keep = False, **filter_current)
        
    return read_data_scm


def read_wide_csv_file_pd(file, folder, col_rename: dict = {}, col_defaults: dict = {}) -> pd.DataFrame:
    """
    This function reads a csv file and returns the data as a pandas Dataframe. It is used by 
    read_wide_csv_folder and can not do the filtering an meta_mapping done by read_wide_csv_file
    
    Columns can be renamed or filled with default vales to match the pyCPA structure.
        
    Currently duplicate datapoints will not be detected
    
    Parameters
    ----------
    
    file : str
        String containing the file name    
    
    folder : str
        String with folder name to read from
    
    col\_rename : dict
        Dict where the keys are column names in the files to be read and the value is the column file 
        in the output dataframe. Use '\DROP' as value to remove a column. Default = empty
    
    col\_defaults : dict
        Dict for default values of columns not given in the csv files. The keys are the column names and 
        the values are the values for the columns. Default: empty
        
    Returns
    -------
    
    :obj:`pandas.DataFrame`
        pandas DataFrame with the read data
        
    """
    
    
    #read_data = pd.DataFrame
    read_data = pd.read_csv(os.path.join(folder, file))
        
    ## rename headers 
    ## include checks and throw errors
    columns = read_data.columns.values 
    for col in col_rename:
        if col_rename[col] == '\DROP':
            read_data.drop(columns = [col], inplace = True)
    
    # columns may have changed!
    columns = read_data.columns.values
    for col in col_rename:
        if col_rename[col] != '\DROP':
            columns[columns == col] = col_rename[col]

    ## add default value columns
    ## TODO include checks if cols already present and throw errors
    for col in col_defaults:
        read_data.insert(0, col, col_defaults[col])
    
    ## if class column exists, rename it because that is a python keyword
    if 'class' in read_data.columns.values:
        read_data.columns.values[read_data.columns.get_loc("class")] = "classification"
    
    ## if classification doesn't exist initialize with 'TOTAL'
    if not 'classification' in read_data.columns.values:
        idx_variable_col = read_data.columns.get_loc("variable")
        read_data.insert(idx_variable_col + 1, 'classification', 'total')
        
    ## replace strings with NaN in year columns
    year_regex = re.compile('\d')
    year_cols = list(filter(year_regex.match, read_data.columns.values))
    
    ## remove all non-numeric values from year columns
    for col in year_cols:
        read_data[col] = read_data[col][pd.to_numeric(read_data[col], errors='coerce').notnull()]
    
    return read_data


def read_wide_csv_folder(file_regex, folder, col_rename: dict = {}, col_defaults: dict = {},
                       meta_mapping: dict = {}, filter_keep: dict = {}, 
                       filter_remove: dict = {}) -> scmdata.run.ScmRun:
    """
    This function reads all csv files that match "file_regex" from folder "folder" and returns 
    them as a single ScmDataFrame 
    
    Columns can be renamed or filled with default vales to match the pyCPA structure.
    The individual files are read using "read_wide_csv_data"
    
    Currently duplicate datapoint will not be detected
    
    Parameters
    ----------
    
    file_regex : str
        String containing a regular expression that selects the files to read. 
        Only files matching this expression will be read.    
    
    folder : str
        String with folder name to read from
    
    col_rename : dict
        Dict where the keys are column names in the files to be read and the value is the column file 
        in the output dataframe. Use '\DROP' as value to remove a column. Default = empty
    
    col_defaults : dict
        Dict for default values of columns not given in the csv files. The keys are the column names and 
        the values are the values for the columns. Default: empty
    
    meta_mapping : dict
        A dict with pyCPA column names as keys. Values are dicts with input values as 
        keys and output values as values. A standard use case is to map gas names from 
        input data to the standardized names used in pyCPA / scmdata. Default: empty
            
    filter_keep : dict
        Dict defining filters of data to keep. Filtering is done after metadata mapping, 
        so use mapped metadata values to define the filter. Column names are pyCPA columns.
        Each entry in the dict defines an individual filter. The names of the filters have no 
        relevance. Default: empty
            
    filter_remove : dict
        Dict defining filters of data to remove. Filtering is done after metadata mapping, 
        so use mapped metadata values to define the filter. Column names are pyCPA columns.
        Each entry in the dict defines an individual filter. The names of the filters have no 
        relevance. Default: empty
        
    
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        ScmRun with the read data
        
        
    Examples
    --------
    
    example for meta\_mapping::
        
        meta_mapping = {
            'pyCPA_col_1': {'col_1_value_1_in': 'col_1_value_1_out',
                            'col_1_value_2_in': 'col_1_value_2_out',
                            },
            'pyCPA_col_2': {'col_2_value_1_in': 'col_2_value_1_out',
                            'col_2_value_2_in': 'col_2_value_2_out',
                            },
        }
            
    example for filter\_keep::
        
        filter_keep = {
            'f_1': {'variable': ['CO2', 'CH4'], 'region': 'USA'},
            'f_2': {'variable': 'N2O'}        
        } # this example filter keeps all CO2 and CH4 data for the USA and N2O data for all countries
        
    example for filter\_remove::
        
        filter_remove = {
            'f_1': {'scenario': 'HISTORY'},
        } # this filter removes all data with 'HISTORY' as scenario
    
    """
   
    print("Reading data from folder " + folder)
        
    # get list of all files in data_folder
    files = os.listdir(folder)
    # filter file list according to given regular expression    
    files = [x for x in files if re.search(file_regex, x)]
    
    if files:
        firstfile = True
        for file in files:
            print('Reading file ' + file + ' using read_wide_csv_data.')
            if firstfile:
                data = read_wide_csv_file_pd(file, folder, col_rename, col_defaults)
                firstfile = False
            else:
                new_data = read_wide_csv_file_pd(file, folder, col_rename, col_defaults)
                #data.append(new_data, inplace = True)
                data = data.append(new_data)
    
        # metadata mapping
        if meta_mapping:
            data.replace(meta_mapping, inplace = True)
        
        # convert to ScmRun
        data_scm = scmdata.run.ScmRun(data)
        
        # filter the data
        for column in filter_keep.keys():
            filter_current = filter_keep[column]
            data_scm.filter(inplace = True, keep = True, **filter_current)
        
        for column in filter_remove.keys():
            filter_current = filter_remove[column]
            data_scm.filter(inplace = True, keep = False, **filter_current)
                
        return data_scm
    else: 
        return None

def read_dataset(config_folder, config_file) -> scmdata.run.ScmRun:
    """
    This function reads a dataset with function and parameters given in the configuration file.
    It is a wrapper function for the different data reading functions.
    The possible functions are currently hard coded for security reasons
    
    Parameters
    ----------
    
    config_folder : str
        Folder holding the config file.
    
    config_file : str
        A yaml file with allparameters for the data reading including folder and function to use
        For an example see 'example_read_wide_csv_folder.yaml' in 'examples/metadata'.
    
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        scmdata.run.Scmrun with the read data
        
    """
    
    # read spec YAML file
    with open(os.path.join(config_folder, config_file)) as stream:
        try:
            config = yaml.load(stream, Loader = yaml.SafeLoader)
        except yaml.YAMLError as exc:
            print(exc)
            return
    
    # check input, function first
    if not 'function'in config.keys():
        raise ValueError("Key 'function' is not defined in yaml config file")
    
    # determine necessary parameters
    if config['function'] == 'read_wide_csv_folder':
        necessary_paras = ['folder', 'file_regex', 'col_rename', 'col_defaults', 'filter_keep', 
                          'filter_remove', 'meta_mapping']
        # currently all keys are necessary because no default value handling im implemented.
        # In the future moveto a library such as voluptuous to do the input checking and 
        # handling of default values
    else:
        raise ValueError('Function ' + config['function'] + ' is not a known pyCPA data reading function')
    
    # check if all necessary parameters are present
    present_paras = config.keys()
    for para in necessary_paras:
        if not para in present_paras:
            raise ValueError("Key " + para + " is not defined in yaml config file")
            
    # now do the actual reading
    if config['function'] == 'read_wide_csv_folder':
        read_data = read_wide_csv_folder(config['file_regex'], config['folder'], 
                                         col_rename = config['col_rename'], 
                                         col_defaults = config['col_defaults'],
                                         filter_keep = config['filter_keep'], 
                                         filter_remove = config['filter_remove'], 
                                         meta_mapping = config['meta_mapping'])
    else:
        # we should not be able to rech this point as this has been checked before
        raise ValueError('Function ' + config['function'] + ' is not a known pyCPA data reading function. ' +
                         'This point should not be reached as this has been checked before.')
        
    return read_data
    
#def read_long_file(file, folder, col_rename, col_defaults, **kwargs) -> scmdata.dataframe.ScmDataFrame:
#    
#    kwarglist = list(kwargs.keys())
#    
#    # determine filetype from ending
#    file_ending = file.split('.')[-1]
#    if file_ending == 'csv':
#        if "encoding" in kwarglist:    
#            data_long = pd.read_csv(os.path.join(folder, file), encoding = kwargs["encoding"]) #,skiprows = specs['skiprows'])
#        else:
#            data_long = pd.read_csv(os.path.join(folder, file))#, skiprows = specs['skiprows'])
#    elif file_ending == 'parquet':
#        data_long = pd.read_parquet(os.path.join(folder, file))
#    else:
#        print('unknown file type: ' + file_ending)
#        return
#    
#        
#    ## rename headers 
#    ## include checks and throw errors
#    columns = read_data.columns.values 
#    for col in col_rename:
#        columns[columns == col] = col_rename[col]
#
#    read_data.columns = columns
#    
#    ## add default value columns
#    ## TODO include checks if cols already present and throw errors
#    for col in col_defaults:
#        read_data.insert(0, col, col_defaults[col])
#    
#    ## if class column exists, rename it because that is a python keyword
#    ## if it doesn't exist initialize with 'TOTAL'
#    if 'class' in read_data.columns.values:
#        read_data.columns.values[read_data.columns.get_loc("class")] = "class_t"
#    else:
#        idx_variable_col = read_data.columns.get_loc("variable")
#        read_data.insert(idx_variable_col + 1, 'class_t', 'TOTAL')
#        
#    
#    ## convert to scmdata dataframe
#    read_scm = scmdata.dataframe.ScmDataFrame(read_data)
#    
#    ## return
#    return read_scm