#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 08:39:51 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""


import scmdata
import pandas as pd
import os
#from pyCPA.core import combine_rows

def check_coverage(input_DF, data_filter, axes_variables, folder: str = '\default', 
                   filename: str = '\default', verbose: bool = False) -> pd.DataFrame:
    """
    Create a table showing how many rows exist for a certain combination of axes_variables. 
    The axes of the table are defined by axes_variables (e.g. category and entity or country 
    and source). data\_filter can be used to filter the dataframe before the analysis. 
    Currently year coverage is not analyzed (ToDo).
    Resulting data will be saved as a csv file and also returned as a pandas DataFrame
    
    Parameters
    ----------
    
    input\_DF
        ScmDataFrame with data to be abalyzed
    data\_filter
        filter in scmdata format (a dict) to filter the data before processing
    axes\_variables
        list with two entries for x and y axis. Each a variable from thew metadata cols of the input\_DF
    folder
        the folder where the result will be saved. If not given the default output folder is used
    filename
        filename for the result. If not given a filename wil be generated 
    verbose
        bool: if set to true a lot of debug output will be written to the terminal
    
    Returns
    -------
    
    :obj:`scmdata.dataframe.ScmDataFrame`
        scmDataFrame with the converted data
        
    """
    
    # filter the input dataframe
    input_DF.filter(keep = True, inplace = True, **data_filter)
    
    # now loop over rows and filter for each row variable. In a nested loop do the same for cols
    # the coverage count is just the number of rows in the filtered dataframe
    
    # get the values for columns
    values_col = input_DF.get_unique_meta(axes_variables[0])
    
    # get the values for rows
    values_rows = input_DF.get_unique_meta(axes_variables[1])
    
        
    # list to collect rows
    all_rows = [] 
    
    for row in values_rows:
        # ToDo: check if first filtering for rows and then for each col is really faster than 
        # filtering for both for each cell
        current_DF = input_DF.filter(keep = True, inplace = False, **{axes_variables[1]: row})
        
        results_this_row = [row]
        for col in values_col:
            DF_this_cell = current_DF.filter(keep = True, inplace = False, **{axes_variables[0]: col})
            shape_DF = DF_this_cell.meta.shape
            results_this_row.append(shape_DF[0])
            
        all_rows.append(results_this_row)
        
    # create coverage_DF
    coverage_DF = pd.DataFrame(data = all_rows, columns = [axes_variables[1] + ' \ ' + axes_variables[0]] + values_col)
        
    # save the result
    #if not (folder == '\none' or filename == '\none'):
        # ToDo: code for saving
   
    return coverage_DF