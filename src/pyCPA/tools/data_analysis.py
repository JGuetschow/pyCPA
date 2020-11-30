#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 08:39:51 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""


import pandas as pd
import os
from datetime import datetime


def check_coverage(input_DF, data_filter, axes_variables, folder: str = '\default', 
                   filename: str = '\default') -> pd.DataFrame:
    """
    Create a table showing how many rows exist for a certain combination of axes_variables. 
    The axes of the table are defined by axes_variables (e.g. category and entity or country 
    and source). data\_filter can be used to filter the dataframe before the analysis. 
    Currently year coverage is not analyzed (ToDo).
    Resulting data will be saved as a csv file and also returned as a pandas DataFrame
    
    Parameters
    ----------
    
    input\_DF
        ScmDataFrame with data to be analyzed
        
    data\_filter
        filter in scmdata format (a dict) to filter the data before processing
        
    axes\_variables
        list with two entries for x and y axis. Each a variable from thew metadata cols of the input\_DF
        
    folder
        the folder where the result will be saved. If not given or set to "\defaukt" the default output 
        folder will be used. If set to "\none" no file will be written.
        
    filename
        filename for the result. If not given or set to "\default" a filename will be generated. 
        If set to "\none" no file will be written.
    
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        scmDataFrame with the converted data
        
    """
    
    # filter the input dataframe
    filtered_DF = input_DF.filter(keep = True, inplace = False, **data_filter)
    
    # now loop over rows and filter for each row variable. In a nested loop do the same for cols
    # the coverage count is just the number of rows in the filtered dataframe
    
    # get the values for columns
    values_col = sorted(filtered_DF.get_unique_meta(axes_variables[0]))
    
    # get the values for rows
    values_rows = sorted(filtered_DF.get_unique_meta(axes_variables[1]))
    
        
    # list to collect rows
    all_rows = [] 
    
    for row in values_rows:
        # ToDo: check if first filtering for rows and then for each col is really faster than 
        # filtering for both for each cell
        current_DF = filtered_DF.filter(keep = True, inplace = False, **{axes_variables[1]: row},
                                                                         log_if_empty = False)
        
        results_this_row = [row]
        for col in values_col:
            DF_this_cell = current_DF.filter(keep = True, inplace = False, **{axes_variables[0]: col},
                                                                              log_if_empty = False)
            shape_DF = DF_this_cell.meta.shape
            results_this_row.append(shape_DF[0])
            
        all_rows.append(results_this_row)
        
    # create coverage_DF
    coverage_DF = pd.DataFrame(data = all_rows, columns = [axes_variables[1] + ' \ ' + axes_variables[0]] + values_col)
        
    # save the result
    if not (folder == '\none' or filename == '\none'):
        if filename == '\default':
            date = datetime.now()
            # the filename could contain more information on the other metadata to identify the content from the name
            filename = 'coverage_' + axes_variables[0] + '_' + axes_variables[1] + '_' + date.strftime("%m_%d_%Y") + '.csv'
        if folder == '\default':
            #TODO make configurable
            folder = 'output'
        if not os.path.isdir(folder):
            os.mkdir(folder, 0o755)
            
        coverage_DF.to_csv(os.path.join(folder, filename), index = False)
   
    return coverage_DF


#def check_consistency(input_DF, tests, columns, folder_test: str = '', data_filter: dict = {}, 
#                      folder_output: str = '\default', filename_output: str = '\default', 
#                      verbose: bool = False) -> pd.DataFrame:
#    """
#    
#    Testing data consistency. Currently tests can only be done within one dimension. 
#    Results are saved to a text file and if desired written to terminal 
#    
#    Parameters
#    ----------
#    
#    input\_DF
#        ScmDataFrame with data to be analyzed
#        
#    tests
#        either: 
#            1: pandas DataFrame containing the tests in a column structure as defined by conversion.             
#            The values of each source column have to be of the format that combine_rows understands, i.e. 
#            value1_whitespace_operator_whitespace_value2 ...
#        or:
#            string with the name of a csv file which has the column structure described below
#            
#    conversion
#        a dict defining the rows to use in the input\_DF the tests DF. Keys are pyCPA column names
#        while the values are source and target column names in the tests dataframe/file.
#        The syntax is the same as in conversion.map_data
#        columns = {
#            "category": ["SourceCategory", "TargetCategory"], 
#            "categoryName": "CRFcategoryName", 
#        }
#            
#    data\_filter
#        filter in scmdata format (a dict) to filter the data before processing
#    folder
#        the folder where the result will be saved. If not given or set to "\defaukt" the default output 
#        folder will be used. If set to "\none" no file will be written.
#    filename
#        filename for the result. If not given or set to "\default" a filename will be generated. 
#        If set to "\none" no file will be written.
#    verbose
#        bool: if set to true a lot of debug output will be written to the terminal
#    
#    Returns
#    -------
#    
#    :obj:`scmdata.dataframe.ScmDataFrame`
#        scmDataFrame with the checks table amended with information on passed and failed checks 
#        
#    """
#    
#    # filter the input dataframe (copy to not change the original dataframe in the calling code)
#    filtered_DF = input_DF.filter(keep = True, inplace = False, **data_filter)
#    
#    # prepare tests 
#    if istype(test, str):
#        # read the table
#        table_test = pd.read_csv(os.path.join(folder, tests)) 
#    
#    
#    mapping_table = 
#    
#    
#    #### old stuff
#    
#    # now loop over rows and filter for each row variable. In a nested loop do the same for cols
#    # the coverage count is just the number of rows in the filtered dataframe
#    
#    # get the values for columns
#    values_col = input_DF.get_unique_meta(axes_variables[0])
#    
#    # get the values for rows
#    values_rows = input_DF.get_unique_meta(axes_variables[1])
#    
#        
#    # list to collect rows
#    all_rows = [] 
#    
#    for row in values_rows:
#        # ToDo: check if first filtering for rows and then for each col is really faster than 
#        # filtering for both for each cell
#        current_DF = input_DF.filter(keep = True, inplace = False, **{axes_variables[1]: row})
#        
#        results_this_row = [row]
#        for col in values_col:
#            DF_this_cell = current_DF.filter(keep = True, inplace = False, **{axes_variables[0]: col})
#            shape_DF = DF_this_cell.meta.shape
#            results_this_row.append(shape_DF[0])
#            
#        all_rows.append(results_this_row)
#        
#    # create coverage_DF
#    coverage_DF = pd.DataFrame(data = all_rows, columns = [axes_variables[1] + ' \ ' + axes_variables[0]] + values_col)
#        
#    # save the result
#    if not (folder == '\none' or filename == '\none'):
#        if filename == '\default':
#            date = datetime.now()
#            # the filename could contain more information on the other metadata to identify the content from the name
#            filename = 'coverage_' + axes_variables[0] + '_' + axes_variables[1] + '_' + date.strftime("%m_%d_%Y") + '.csv'
#        if folder == '\default':
#            #TODO make configurable
#            folder = 'output'
#        if not os.path.isdir(folder):
#            os.mkdir(folder, 0o755)
#            
#        coverage_DF.to_csv(os.path.join(folder, filename), index = False)
#   
#    return coverage_DF