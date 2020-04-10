#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:02:19 2020

@author: johannes
"""

#import pandas as pd
import scmdata
#import os
import itertools
import re

########
#### function to convert given PRIMAP IPCC 1996 / 2006 / CRF code to pyCPA format
########    
def convert_IPCC_code_PRIMAP_to_pyCPA(code) -> str:
    # this function converts IPCC emissions category codes from the PRIMAP-format to
    # the pyCPA format which is closer to the original (without all the dots)

    arabic_to_roman = {
        '1': 'i',
        '2': 'ii',
        '3': 'iii',
        '4': 'iv',
        '5': 'v',
        '6': 'vi',
        '7': 'vii',
        '8': 'viii',
        '9': 'viiii'
    }
    
    # TODO: checks at all levels (currently only a few)

    if len(code) < 4:
        # code too short 
        # TODO: throw error
        print('Category code ' + code + ' is too short. Not converted') 
    elif code[0 : 3] in ['IPC', 'CAT']:
        # it's an IPCC code. convert it
        # check if it's a custom code (beginning with 'M'). Currently these are the same in pyCPA as in PRIMAP
        if code[3] =='M':
            new_code = code[3 :]
        else:
            # actual conversion happening here
            ## only work with the part without 'IPC' or 'CAT'
            code_remaining = code[3 :]

            ## first two chars are unchanged
            if len(code) in [1, 2]:
                new_code = code_remaining
            else: 
                new_code = code_remaining[0 : 2]
                code_remaining = code_remaining[2 : ] 
                #print('code=' + new_code + ' code_remaining=' + code_remaining)
                # the next part is a number. match by regexp to also match 2 digit numbers (just in case there are any, currently not needed)
                match = re.match('[0-9]*', code_remaining)
                if match is None:
                    # code does not obey specifications. Throw a warning and stop conversion
                    # TODO: throw warning
                    print('Category code ' + code + ' does not obey spcifications. No number found on third level')
                    new_code = ''
                else:    
                    new_code = new_code + match.group(0)
                    code_remaining = code_remaining[len(match.group(0)) : ]
                    #print('code=' + new_code + ' code_remaining=' + code_remaining)

                    # fourth level is a char. Has to be transformed to lower case
                    if len(code_remaining) > 0:
                        new_code = new_code + code_remaining[0].lower()
                        code_remaining = code_remaining[1 : ]
                        #print('code=' + new_code + ' code_remaining=' + code_remaining)

                        # now we have an arabic numeral in the PRIMAP-format but a roman numeral in pyCPA
                        if len(code_remaining) > 0:
                            new_code = new_code + arabic_to_roman[code_remaining[0]]
                            code_remaining = code_remaining[1 :]
                            #print('code=' + new_code + ' code_remaining=' + code_remaining)

                            # now we have a number again. An it's the end of the code. So just copy the rest
                            new_code = new_code + code_remaining
                            #print('code=' + new_code + ' code_remaining=' + code_remaining)

        return new_code
    
    else:
        # the prefix is missing. Throw a warning and don't do anything
        # TODO: throw warning
        print('Category code ' + code + ' is not in PRIMAP IPCC code format. Not converted')
        return ''

        
########
#### function to convert PRIMAP IPCC 1996 / 2006 / CRF categories to pyCPA format in data frame
########
def convert_IPCC_categories_PRIMAP_to_pyCPA(data_frame):
    # this function converts IPCC emissions category codes from the PRIMAP-format to
    # the pyCPA format which is closer to the original (without all the dots)
    
    all_codes = data_frame.get_unique_meta('category')
    
    replacements = dict()
    
    for code in all_codes:
        replacements[code] = convert_IPCC_code_PRIMAP_to_pyCPA(code)
    
    data_frame.rename({'category': replacements}, inplace = True)
    

########
#### function to add GWP column from variable names
########
def add_GWP_information(data_frame, defaultGWP):
    # this function takes GWP information from the variable name (if present) and uses the default given otherwise
    # the default is given as "AR4", "SAR","AR5" etc (as in the unit names)
    # the function has to be used before "convert_unit_PRIMAP_to_scmdata"
    # TODO integrate check for converted units (or integrate this function in the unit conversion)

    units_GWP = ['KYOTOGHG', 'HFCS', 'PFCS', 'FGASES']

    # define the mapping of PRIMAP GWP scefications to scmdata GWP specification
    # no GWP given will be mapped to SAR
    GWP_mapping = {
        'SAR': 'SARGWP100',
        'AR4': 'AR4GWP100',
        'AR5': 'AR5GWP100',
    }

    # first check if "unit_context" column already exists. If so exit
    if "unit_context" in data_frame.meta.columns.values:
        print("unit_context columns already existing. Exiting")
    else:
        # get all units in the dataframe
        all_variables = data_frame.get_unique_meta('variable')

        # copy the unit col to "unit_context" to then work with replacements
        idx_unit_col = data_frame.meta.columns.get_loc("unit")
        data_frame._meta.insert(idx_unit_col + 1, 'unit_context', '')
        data_frame._meta["unit_context"] = data_frame._meta["variable"] # + ' ' + defaultGWP

        # build regexp to match the GWP conversion variables
        regexp_str = '('
        for unit in units_GWP:
            regexp_str = regexp_str + unit  + '|' 
        regexp_str = regexp_str[0 : - 1] + ')'

        # build regexp to match the GWP conversion variables
        regexp_str_GWPs = '('
        for mapping in GWP_mapping.keys():
            regexp_str_GWPs = regexp_str_GWPs + mapping  + '|' 
        regexp_str_GWPs = regexp_str_GWPs[0 : - 1] + ')$'

        # initialize GWP replacements with replacements for all variables which do not contain GWP information
        GWP_replacements = dict()
        variable_replacements = dict()
        regex_GWP_units = re.compile(regexp_str)
        variables_GWP = list(filter(regex_GWP_units.search, all_variables))
        #print(variables_GWP)
        variables_other = list(set(all_variables) - set(variables_GWP))
        for variable in variables_other:
            GWP_replacements[variable] = GWP_mapping[defaultGWP]


        for variable in variables_GWP:
            match = re.search(regexp_str_GWPs, variable)
            if match is None:
                # SAR GWPs are default in PRIMAP
                currentGWP = 'SAR'
            else:
                currentGWP = match.group(0)
                # in this case the cariable has to be replaced as well
                match = re.search('.*(?=' + currentGWP + ')', variable)
                if match is None:
                    # throw error this should be impossible
                    print('Error in GWP processing')
                else:
                    variable_replacements[variable] = match.group(0)

            GWP_replacements[variable] = GWP_mapping[currentGWP]

        # make the replacement
        data_frame.rename({'variable': variable_replacements, 'unit_context': GWP_replacements}, inplace = True)
    

########
#### function to bring units in scmdata format
########
def convert_unit_PRIMAP_to_scmdata(data_frame):
    ## the entity has to be part of the unit and also the time frame
    ## as the emissions module output can also have units which contain a substance (e.g. GtC)
    ## we need to detect those

    # define exceptions
    ## specialities
    exception_units = {
        'CO2eq': 'CO2', # convert to just CO2 
        'N': '<variable>N', # currently only implemented for N2O (untested)
        'C': 'C' # don't add variable here (untested)
    }
    
    ## basic units
    basic_units = ['g', 't']

    ## prefixes 
    SI_unit_multipliers = ['k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'] 

    ## combines basic units with prefixes
    units_prefixes = list(itertools.product(SI_unit_multipliers, basic_units))
    units_prefixes = [i[0] + i[1] for i in units_prefixes]
    
    # time information to add
    time_frame_str = ' / yr';
    
    # build regexp to match the basic units with prefixes in units
    regexp_str = '('
    for unit in units_prefixes:
        regexp_str = regexp_str + unit  + '|' 
    regexp_str = regexp_str[0 : - 1] + ')' #'(.*)'
    
    # add variable_unit column with replaced units.
    ## special units will be replaced later
    idx_unit_col = data_frame.meta.columns.get_loc("unit")
    data_frame._meta.insert(idx_unit_col + 1, 'variable_unit', '')
    data_frame._meta["variable_unit"] = data_frame._meta["unit"] + ' ' + data_frame._meta["variable"] + time_frame_str
    
    # get unique units
    unit_values = data_frame.get_unique_meta('unit')
    present_exception_units = []
    replacements = dict()
    for exception_unit in list(exception_units.keys()):
        #print(exception_unit)
        # TODO this needs error handling (e.g. when nothing has been matched)
        CRF_current_unit = data_frame.filter(unit = regexp_str + exception_unit + '$', regexp=True)
        # get unique values of variable unit combinations
        units_current_unit = CRF_current_unit.get_unique_meta('variable_unit')
        # for each create a dict entry for conversion
        #print(units_current_unit)
        for current_var_unit in units_current_unit:
            # first get the prefix and basic unit (e.g. Gt)
            match = re.search(regexp_str, current_var_unit)
            pref_basic = match.group(0)

            # then get variable
            match = re.search('(?<=\s)(.*)(?=\s/\s)', current_var_unit)
            variable = match.group(0)

            # now build the replacement 
            replacement_str = pref_basic + ' ' + exception_units[exception_unit] + time_frame_str
            # add the variable if necessary
            replacements[current_var_unit] = replacement_str.replace('<variable>', variable) # untested as N replacement not tested yet
    
    # now do the replacement
    data_frame.rename({'variable_unit': replacements}, inplace = True)

    # delete unit col
    data_frame._meta.drop('unit', 1, inplace = True)

    # rename column 'variable_unit' to 'unit'
    data_frame.meta.columns.values[data_frame.meta.columns.get_loc("variable_unit")] = 'unit'

########    
#### function to combine data for different values of metadata
########
def combine_rows(data_frame, column, values_to_combine, new_value, other_cols, operator, 
                 cols_to_remove: list = [], inplace: bool = True, verbose: bool = False) -> scmdata.dataframe.ScmDataFrame:
    # this function combines rows for given values of a meta data columns where all other values coincide
    # with other_cols (a dict) a subset of the dataset can be defined which the function oeprates on
    # operator will be used for all but the first values_to_combine (first always has positive sign) 
    
    # currently only add and substract operators are implemented
    # TODO iplement checks for operator
              
    all_active_col_values = data_frame.get_unique_meta(column)
    present_values_to_combine = list(set(all_active_col_values).intersection(set(values_to_combine)))
        
    if len(present_values_to_combine) == 0:
        # throw warning
        print('No data for any value to combine of column ' + column)
        #if not inplace:
        #    return 
    else:
        # first filter the dataset such that we only work on the data defined by other_cols and other_col_values
        # we also don't need the rows which have vales of "column" which are not in values_to_combine
        col_filter = other_cols.copy()
        values_to_combine_and_target = values_to_combine.copy()
        if inplace: 
            values_to_combine_and_target.append(new_value)
            col_filter[column] = values_to_combine_and_target
        else:
            col_filter[column] = values_to_combine
            
        data_to_work_with = data_frame.filter(keep = True, inplace = False, **col_filter)
        
        if data_to_work_with.timeseries().empty:
            print('No data left after filtering for other column values')
            return
        
        # remove this data from the main data_frame. The current approch saves memory but looses data if function crashes.
        # one could also work on a copy and return the copy only if all worked out
        # don't remove as we might want additional rows not replacement of rows
        # data_frame.filter(keep = False, inplace = True, **col_filter)

        # now split existing result time series from rest of data_to_work_with
        if not inplace:
            col_filter[column] = new_value
            existing_result_timeseries = data_to_work_with.filter(keep = True, inplace = False, **col_filter)
            # check if result timeseries already exists (currently not used)
            if existing_result_timeseries.timeseries().empty:
                result_existing = False
            else:
                result_existing = True
        
        col_filter[column] = values_to_combine
        data_to_work_with.filter(keep = True, inplace = True, **col_filter)
        if verbose:
            print(data_to_work_with.head())
           

        # check if all units are the same
        all_units = data_to_work_with.get_unique_meta('unit')
        if len(all_units) > 1:
            # we need unit conversion
            # check if all variables are the same
            all_vars = data_to_work_with.get_unique_meta('variable')
            if len(all_vars) > 1:
                # now we have two cases: 
                # 1) the variable is operated on and we need CO2 equivalence
                # 2) the variable is a group column and we need unit conversion per variable
                
                if column == 'variable':
                    # 1) CO2 equivalence is needed
                    unit_to = 'Mt CO2 / yr'
                    # get all contexts present
                    all_contexts = data_to_work_with.get_unique_meta('unit_context')

                    for current_context in all_contexts:
                        # convert data
                        if verbose:
                            print('converting for context ' + current_context)
                            print('unit_to: ' + unit_to)
                        data_to_work_with.convert_unit(unit_to, context = current_context, inplace = True, 
                                                      **{'unit_context': current_context})
                        #print(data_to_work_with.head())
                        
                else:
                    # 2) no CO2 equivalence but individual conversion for each variable
                    for variable in all_vars:
                        if verbose:
                            print('converting for variable ' + variable) 
                        data_this_var = data_to_work_with.filter(keep = True, inplace = False, 
                                                                 **{'variable': variable})
                        all_units_this_var = data_this_var.get_unique_meta('unit')
                        if verbose:
                            print(all_units_this_var)
                        if len(all_units_this_var) > 1:
                            unit_to = all_units_this_var[0]
                            if verbose:
                                print('converting to ' + unit_to)
                            # convert data
                            # TODO: contexts have to be considered? Or does that happen automatically because they are stored in the dataframe
                            data_to_work_with.convert_unit(unit_to, inplace = True, **{'variable': variable})
                    
            else: # not tested so far
                unit_to = all_units[0]
                # convert data
                # TODO: contexts have to be considered? Or does that happen automatically because they are stored in the dataframe
                data_to_work_with.convert_unit(unit_to, inplace = True)

        # prepare data in case of subtractions
        # first check if we have individual operators for all values_to_combine
        if type(operator) is list: 
            # check if list has the correct length (move to beginning of function)
            # also add a check if it's just '+' and '-'
            if len(operator) != len(values_to_combine):
                # throw error
                # TODO: throw error
                print('operator list and values_to_combine have different lengths')
                print(operator)
                print(values_to_combine)
                return
            else:
                # find the entries with '-'
                subtract_values = []
                for i_operator in range(0, len(operator)):
                    if operator[i_operator] == '-':
                        subtract_values.append(values_to_combine[i_operator])
        else:
            # if only one value_to_combine apply operator to that.
            # if more than one value,apply to all but the first
            if len(values_to_combine) == 1:
                subtract_values = values_to_combine
            else:
                subtract_values = values_to_combine[1 :]
        
        # now apply - to all rows with column values in subtract_values (if there are any)
        if subtract_values:
            print(subtract_values)
        
            data_to_change_sign = data_to_work_with.filter(inplace = False, keep = True, 
                                                           **{column: subtract_values})
            data_to_work_with.filter(inplace = True, keep = False, **{column: subtract_values})

            data_to_change_sign_TS = data_to_change_sign.timeseries().apply(lambda x: - x)
            data_changed_sign = scmdata.dataframe.ScmDataFrame(data_to_change_sign_TS)

            data_to_work_with.append(data_changed_sign,inplace = True)
       
                
        # use groupby to create   
        ## first find all cols that have non-unique values (except for the colum to operate on)
        all_columns = data_to_work_with.meta.columns.values
        all_columns_to_check = list(set(all_columns) - set([column]))

        group_cols = []
        unique_cols = dict()

        for current_column in all_columns_to_check:
            values_this_col = data_to_work_with.get_unique_meta(current_column)
            if verbose:
                print(current_column)
                print(values_this_col)
            if len(values_this_col) > 1:
                group_cols.append(current_column)
            else:
                unique_cols[current_column] = values_this_col[0]

        if len(group_cols) == 0:
            # what to do if we want to work on regions and there is only one present. need another col in this case
            group_cols.append('region')
        
        if 'unit' in group_cols:
            if 'variable' in group_cols:
                all_variables = data_to_work_with.get_unique_meta('variable')
                for variable in all_variables:
                    data_this_var = data_to_work_with.filter(keep = True, inplace = False, **{'variable': variable})
                    all_units_this_var = data_this_var.get_unique_meta('unit')
                    if len(all_units_this_var) > 1:
                        # throw error
                        # TODO: throw error
                        print('Unit col has non-unique metadata for variable ' + variable + 
                              '. Something went wrong with unit conversion.')
                        return
            else:
                # throw error
                # TODO: throw error
                print('Unit col has non-unique metadata. Something went wrong with unit conversion.')
                return
            
        units = data_to_work_with.get_unique_meta('unit')
        if verbose:
            print('unit values:', *units, sep= ', ' )
            print('Grouping on columns:', *group_cols, sep = ', ')
        # apply the operation (currently only sum)
        result_DF = data_to_work_with.timeseries().groupby(group_cols).sum()

        ## add the meta columns back in. they were removed by groupby 
        ## get all col values
        columns = data_to_work_with.meta.columns.values
        group_cols.append(column)
        columns = list(set(columns) - set(group_cols) - set(cols_to_remove))
        if verbose:
            print(columns)
        for i_column in range(0, len(columns)):

            values_this_col = data_to_work_with.get_unique_meta(columns[i_column])
            if len(values_this_col) > 1:
                # TODO: throw exception
                result_DF.insert(i_column, columns[i_column], values_this_col[0])
            else:
                result_DF.insert(i_column, columns[i_column], values_this_col[0])

        result_DF.insert(len(columns), column, new_value)

        # make a new ScmDataFrame 
        result_scmdata = scmdata.dataframe.ScmDataFrame(result_DF)

        # append to data_frame
        if inplace:
            # remove existing data before appending
            # the best way would be to just remove data that's actually added. 
            # Here we remove all data that's potentially added (i.e. the target value for column
            # and the other_values for the ter columns
            col_filter[column] = new_value
            data_frame.filter(keep = False, inplace = True, **col_filter)
            data_frame.append(result_scmdata, inplace = True)
        else:
            return result_scmdata