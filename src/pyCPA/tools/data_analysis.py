#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 08:39:51 2020

@author: Johannes Gütschow, mail@johannes-guetschow.de
"""


import pandas as pd
import numpy as np
import os
from datetime import datetime
import re
from pyCPA.core import combine_rows


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
    
    :obj:`pandas.DataFrame`
        pandas DataFrame with the coverage information
        
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


def check_consistency(input_DF, tests, columns, folder_test: str = '', data_filter: dict = {}, 
                      pass_threshold: float = 0.001, table_col: str = 'variable',
                      table_cell: str = 'region',
                      folder_output: str = '\default', filename_table: str = '\default', 
                      filename_log: str = '\default', verbose: bool = False) -> pd.DataFrame:
    """
    
    Testing data consistency. Results are saved to a csv file, a log file is written and the csv 
    file contents are also returned as a pandas DataFrame 
    
    Parameters
    ----------
    
    input\_DF
        ScmDataFrame with data to be analyzed
        
    tests
        either: 
            1: pandas DataFrame containing the tests in a column structure as defined by conversion.             
            The values of each source column have to be of the format that combine_rows understands, i.e. 
            value1_whitespace_operator_whitespace_value2 ...
        or:
            string with the name of a csv file which has the column structure described below
            
    columns
        a dict defining the rows to use in the input\_DF the tests DF. Keys are pyCPA column names
        while the values are source and target column names in the tests dataframe/file.
        The syntax is the same as in conversion.map_data
        columns = {
            "category": ["SourceCategory", "TargetCategory"], 
            "categoryName": "CRFcategoryName", 
        }
    
    folder\_test
        String with the folder where the tests file resides. Default: ''
        
    data\_filter
        filter in scmdata format (a dict) to filter the data before processing
        
    pass\_threshold
        float defining the threshold for a test to pass. It is compared to the realtive deviation 
        of the calculated time series from the existing time series.
    
    table\_col
    
    table\_cell
    
    folder\_output
        the folder where the result will be saved. If not given or set to "\default" the default output 
        folder will be used. If set to "\none" or '' no file will be written.
        
    filename\_table
        filename for the resulting table in csv format. If not given or set to "\default" 
        a filename will be generated. If set to "\none" or '' no file will be written.
        Default: \default
        
    filename\_log
        filename for the log file. If not given or set to "\default" 
        a filename will be generated. If set to "\none" or '' no file will be written. 
        Default: \default 
        
    verbose
        bool: if set to true all output and additional debugging information will be written to terminal
    
    Returns
    -------
    
    :obj:`pandas.DataFrame`
        pandas DataFrame with the table containing information on failed tests 
        
    """
    
    # filter the input dataframe (copy to not change the original dataframe in the calling code)
    filtered_DF = input_DF.filter(keep = True, inplace = False, **data_filter, log_if_empty = False)
    
    # prepare tests 
    if isinstance(tests, str):
        # read the table
        table_tests = pd.read_csv(os.path.join(folder_test, tests))
    else:
        table_tests = tests.copy()
   
    if not folder_output in ['\none', ''] and not filename_log in ['\none', '']:
        logging = True
    
    allowed_operators = ['+', '-']
    
    # prep for tests
    column_names = columns.keys()
    values_table = filtered_DF.get_unique_meta(table_col)
    n_values = len(values_table)
    all_columns = filtered_DF.meta.columns.values
    all_columns_to_check = list(set(all_columns) - set(column_names) - set([table_col]))
    columns_compare = list(set(all_columns_to_check) - set(['unit', 'unit_context']))
    available_years_str = input_DF.time_points.years().astype(str)
    available_years = input_DF.time_points.values
    
    if logging:
        log = ['Test table header:' ', '.join(table_tests.columns)]
    row_results = []
        
    # loop over entires of mapping table
    for iRow in range(0, len(table_tests)):
        if verbose:
            print('#########################')
            print('Working on row ' + str(iRow))
            print(table_tests.iloc[iRow]) 
            print('#########################')
        
        if logging:
            log.append('Test ' + '{}'.format(iRow) + ': row:' + ', '.join(table_tests.iloc[iRow]))
            
        # prepare for the combination of data
        combo = {}
        result_filter = {}
        for column in column_names:
            # for each column create the entry in the combination dict
            # first get input values
            if columns[column][0] == '*':
                input_values = ['*']
                operator = '+'
            else:
                # get source_code from test table and analyze it
                from_value_current = table_tests[columns[column][0]].iloc[iRow]
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
                        message = 'Illegal operator found for column ' + column + ' in ' 
                        + from_value_current + ', row {:d}'.format(iRow)
                        if verbose:
                            print(message)
                        if logging:
                            log.append(message)
                        break

            # get the output value
            to_value_current = table_tests[columns[column][1]].iloc[iRow]
            # convert to str if it's not
            if not isinstance(to_value_current, str):
                # convert to str
                to_value_current = str(to_value_current)

            # check if conversion for column makes sense     
            if to_value_current in ['None', '', 'nan']:
                if from_value_current in ['None', '', 'nan']:
                    # from also empty, then ignore column
                    message = 'Test for column ' + column + ' empty for row {:d}'.format(iRow)
                    if verbose:
                        print(message)
                    if logging:
                        log.append(message)
                    row_results.append([iRow] + ['Inc'] * n_values)
                    break
                        
                else:
                    # to is empty, from not. That is an error in the mapping table
                    message = 'Inconsistent test table table for ' + column + ', row {:d}'.format(iRow)
                    if verbose:
                        print(message)
                    if logging:
                        log.append(message)
                    row_results.append([iRow] + ['Inc'] * n_values)
                    break
            else:
                if from_value_current in ['None', '','nan']:
                    # from is empty, to not. That is an error in the mapping table
                    message = 'Inconsistent test table for ' + column + ', row {:d}'.format(iRow)
                    if verbose:
                        print(message)
                    if logging:
                        log.append(message)
                    row_results.append([iRow] + ['Inc'] * n_values)
                    break
                else:
                    combo[column] = [input_values, operator, to_value_current]
                    result_filter[column] = to_value_current
        
        # check if the resulting cobination dict is not empty
        if not combo:
            message = 'Combination dict is empty for row {:d}'.format(iRow)
            if verbose:
                print(message)
            if logging:
                log.append(message)
            row_results.append([iRow] + ['Empt'] * n_values)
        else:
            # prepare and make a call to map the data and combine if necessary
            combined_data = combine_rows(filtered_DF, combo, {}, inplace = False, verbose = verbose)
            
            if combined_data.shape[0] > 0:
                if verbose:
                    print('Generated data for row {:d}'.format(iRow) + '. combo dict:')
                    print(combo)
                
                # now loop over the values of the table_col
                results_this_row = [iRow]
                for value in values_table:
                    combined_data_current = combined_data.filter(keep = True, inplace = False, 
                                                                 **{table_col: value}, log_if_empty = False)
                    if combined_data_current.shape[0] > 0:
                        # get the existing data from DF
                        result_filter_current = result_filter
                        result_filter_current[table_col] = value
                        
                        existing_data = filtered_DF.filter(keep = True, inplace = False, **result_filter, 
                                                          log_if_empty = False)
                        if existing_data.shape[0] > 0:
                            # first step is to check if we have time-series for the same meta data values
                            # columns should be the same for combined and existing data as we work on the 
                            # same dataframe
                            group_cols_comb = []
                            unique_cols_comb = dict()
                            group_cols_ex = []
                            unique_cols_ex = dict()

                            for current_column in all_columns_to_check:
                                values_this_col = combined_data_current.get_unique_meta(current_column)
                                if len(values_this_col) > 1:
                                    group_cols_comb.append(current_column)
                                else:
                                    unique_cols_comb[current_column] = values_this_col[0]

                                values_this_col = existing_data.get_unique_meta(current_column)

                                if len(values_this_col) > 1:
                                    group_cols_ex.append(current_column)
                                else:

                                    unique_cols_ex[current_column] = values_this_col[0]
                            
                            # check if group cols and unique cols are the same for combined and result data
                            # if not we don't compare (it would be possible to compare to e.g. data in different
                            # GWP specs, but we don't implement that here at this point)
                            if not group_cols_comb == group_cols_ex:
                                message = ('Group columns differ for combined and existing data. ' 
                                           + 'Comparing not implemented for this case. '
                                           + 'Cols for combined data: ' + ', '.join(group_cols_comb) 
                                           + '; Cols for existing data'  + ', '.join(group_cols_ex))

                                # output and save information
                                if verbose:
                                    print(message)
                                if logging:
                                    log.append(message)

                                results_this_row.append('GC_mm')
                                # move to next value
                                continue
                        
                            if not unique_cols_comb == unique_cols_ex:
                                message = ('Unique columns differ for combined and existing data. ' 
                                           + 'Comparing not implemented for this case. '
                                           + 'Cols for combined data: ' + ', '.join(unique_cols_comb) 
                                           + '; Cols for existing data'  + ', '.join(unique_cols_ex))

                                # output and save information
                                if verbose:
                                    print(message)
                                if logging:
                                    log.append(message)

                                results_this_row.append('UC_mm')
                                # move to next value
                                continue
                            else:
                                message = ('Following columns have unique values: \n'
                                          + ', '.join(['{0}: {1}'.format(val, combined_data_current.meta[val].iloc[0]) for val in 
                                                       unique_cols_comb]))
                                if verbose:
                                    print(message)
                                if logging:
                                    log.append(message)
                            
                            # now we know that the same columns have unique and non-unique values for the 
                            # combined and existing data. we don't know if the same metadata values are 
                            # present. As we want to be able to compare data in different units and GWP 
                            # specification the columns to compare don't contain unit and unit_context
                            comp_col_combinations_ex = existing_data.meta[columns_compare]
                            comp_col_combinations_comb = combined_data_current.meta[columns_compare]
                            unique_CC_ex = comp_col_combinations_ex.drop_duplicates()
                            unique_CC_comb = comp_col_combinations_comb.drop_duplicates()
                        
                            # prepare to compare
                            unique_CC_ex_set = set(map(tuple, unique_CC_ex))
                            unique_CC_comb_set = set(map(tuple, unique_CC_comb))
                            unique_CC_diff = unique_CC_ex_set ^ unique_CC_comb_set # symmetric difference
                        
                            if unique_CC_diff:
                                message = ('Number of unique metadata combination differs for combined and existing data. '
                                           + 'Comparing not implemented for this case. '
                                           + 'Rows for combined data: ' + '{}'.format(len(unique_CC_comb)) 
                                           + '; Rows for existing data'  + '{}'.format(len(unique_CC_ex)))
                                # output and save information
                                if verbose:
                                    print(message)
                                    print('unique_CC_ex')
                                    print(unique_CC_ex)
                                    print('unique_CC_comb')
                                    print(unique_CC_comb)
                                if logging:
                                    log.append(message)
                                    log.append('unique_CC_ex:')
                                    for row in unique_CC_ex:
                                        log.append(', '.join(row))
                                    log.append('unique_CC_comb:')
                                    for row in unique_CC_comb:
                                        log.append(', '.join(row))
                                    
                                results_this_row.append('RC_mm')
                                # move to next value
                                continue
                            
                            
                            
                            # now we have the same metadata, so we can compare the two sets
                            data_diff = existing_data.subtract(combined_data, {})
                            data_sum = existing_data.add(combined_data, {})
                            rel_deviation = data_diff.divide(data_sum, {})
                            
                            failed = rel_deviation.values > pass_threshold / 2
                            if failed.any():
                                message = 'Test failed' 
                                if verbose:
                                    print(message)
                                if logging:
                                    log.append(message)
                                # we have failed tests now we have to go row by row
                                failed_row_idx = np.apply_along_axis(any, 1, failed)
                                failed_rows_failed = failed[failed_row_idx]
                                failed_rows_deviation = rel_deviation.timeseries().iloc[failed_row_idx]
                                idx_reduced = failed_rows_deviation.droplevel(list(unique_cols_comb.keys())) 
                                results_this_cell = []
                                for iRow in range(0, len(failed_rows_failed)):
                                    # table_cell metadata values
                                    results_this_cell.append(failed_rows_deviation.index.get_level_values(table_cell)[iRow])
                                    # now the detailed output for log and verbose
                                    message = (', '.join(idx_reduced.index[iRow]) + ':\n' 
                                               + ', '.join(['{0}: {1:0.1f}'.format(val[0], val[1] * 2) for val in 
                                                            zip(available_years_str[failed_rows_failed[iRow]], 
                                                                failed_rows_deviation[available_years[failed_rows_failed[iRow]]].iloc[iRow].values)]))
                                               
                                    if verbose:
                                        print(message)
                                    if logging:
                                        log.append(message)
                            else:
                                results_this_cell = ['-']

                            results_this_row.append(', '.join(results_this_cell))
                        
                        else: # existing_data is empty
                            message = ('no existing data for value ' + value) 
                            # output and save information
                            if verbose:
                                print(message)
                            if logging:
                                log.append(message)
                            results_this_row.append('no_ED')    
                    
                    else: #combined_data_current is empty
                        message = ('no combined data for value ' + value) 
                        # output and save information
                        if verbose:
                            print(message)
                        if logging:
                            log.append(message)
                        results_this_row.append('no_CD')
                    
                # now add the row with the results to the result DF
                row_results.append(results_this_row)
                    
            else: # combined data is none
                message = 'No combined data for this test.'
                if verbose:
                    print(message)
                if logging:
                    log.append(message)
                row_results.append([iRow] + ['no_CD'] * n_values)

    # create dataframe from list of results
    results_DF = pd.DataFrame(data = row_results, columns = ['test \ ' + table_col] + values_table)
    
    # save the result
    # the table
    if not folder_output in ['\none', ''] and not filename_table in ['\none', '']:
        if filename_table == '\default':
            date = datetime.now()
            # the filename could contain more information on the other metadata to identify the content from the name
            filename_table = 'test_' + table_col + '_' + table_cell + '_' + date.strftime("%m_%d_%Y") + '.csv'
        if folder_output == '\default':
            #TODO make configurable
            folder_output = 'output'
        if not os.path.isdir(folder_output):
            os.mkdir(folder_output, 0o755)
            
        results_DF.to_csv(os.path.join(folder_output, filename_table), index = False)
    
    # the log
    if not folder_output in ['\none', ''] and not filename_log in ['\none', '']:
        if filename_log == '\default':
            date = datetime.now()
            # the filename could contain more information on the other metadata to identify the content from the name
            filename_log = 'log_test_' + table_col + '_' + table_cell + '_' + date.strftime("%m_%d_%Y") + '.log'
        if folder_output == '\default':
            #TODO make configurable
            folder_output = 'output'
        if not os.path.isdir(folder_output):
            os.mkdir(folder_output, 0o755)
    
        with open(os.path.join(folder_output, filename_log), "w") as outfile:
            outfile.write("\n".join(str(row) for row in log))
        
    return results_DF
    
    

    
    
    