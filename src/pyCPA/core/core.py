#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 14:02:19 2020

@author: johannes
.. highlight:: python
"""

import scmdata
#import os
import itertools
import re

########
#### function to convert given PRIMAP IPCC 1996 / 2006 / CRF code to pyCPA format
########    
def convert_IPCC_code_PRIMAP_to_pyCPA(code) -> str:
    """
    This function converts IPCC emissions category codes from the PRIMAP-format to
    the pyCPA format which is closer to the original (but without all the dots)
    
    Codes that are not part of the official hierarchy (starting with IPCM or CATM) 
    are not converted but returned without the 'CAT' or 'IPC' prefix unless the conversion is
    explicitly defined in the code_mapping dict.
    
    Parameters
    ----------
    
    code
        String containing the IPCC code in PRIMAP format (IPCC1996 and IPCC2006 can be converted)
        The PRIMAP format codes consist of upper case letters and numvers only. These are converted 
        back to the original formatting which includes lower case letters and roman numerals. 
        The dots are omitted in both farmattings (ToDo maybe change and add them back in)
    
    Returns
    -------
    
    :str:
        string containing the category code in pyCPA format
        
    Examples
    --------
    
    input: 
        code = 'IPC1A3B34'
    
    output:
        '1A3b3iv'
        
    """

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
    
    code_mapping = {
        'MAG': 'M.AG',
        'MAGELV': 'M.AG.ELV',
        'MBK': 'M.BK',
        'MBKA': 'M.BK.A',
        'MBKM': 'M.BK.M',
        'M1A2M': 'M.1.A.2.m',
        'MLULUCF': 'M.LULUCF',
        'MMULTIOP': 'M.MULTIOP',
        'M0EL': 'M.0.EL',
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
            if new_code in code_mapping.keys():
                new_code = code_mapping[new_code]
            
        else:
            # actual conversion happening here
            ## only work with the part without 'IPC' or 'CAT'
            code_remaining = code[3 :]

            ## first two chars are unchanged but a dot is added
            if len(code_remaining) == 1:
                new_code = code_remaining
            elif len(code_remaining) == 2:
                new_code = code_remaining[0] + '.' + code_remaining[1]
            else: 
                new_code = code_remaining[0] + '.' + code_remaining[1]
                code_remaining = code_remaining[2 : ] 
                # the next part is a number. match by regexp to also match 2 digit numbers (just in case there are any, currently not needed)
                match = re.match('[0-9]*', code_remaining)
                if match is None:
                    # code does not obey specifications. Throw a warning and stop conversion
                    # TODO: throw warning
                    print('Category code ' + code + ' does not obey spcifications. No number found on third level')
                    new_code = ''
                else:    
                    new_code = new_code + '.' + match.group(0)
                    code_remaining = code_remaining[len(match.group(0)) : ]
                    
                    # fourth level is a char. Has to be transformed to lower case
                    if len(code_remaining) > 0:
                        new_code = new_code + '.' + code_remaining[0].lower()
                        code_remaining = code_remaining[1 : ]
                        
                        # now we have an arabic numeral in the PRIMAP-format but a roman numeral in pyCPA
                        if len(code_remaining) > 0:
                            new_code = new_code + '.' + arabic_to_roman[code_remaining[0]]
                            code_remaining = code_remaining[1 :]
                            
                            if len(code_remaining) > 0:
                                # now we have a number again. An it's the end of the code. So just copy the rest
                                new_code = new_code + '.' + code_remaining
                                
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
    """
    This function converts all IPCC emissions category codes in the input data frame 
    from the PRIMAP-format to the pyCPA format which is closer to the original 
    (but without all the dots)
    
    The PRIMAP format codes consist of upper case letters and numvers only. These are converted 
    back to the original formatting which includes lower case letters and roman numerals. 
    The dots are omitted in both farmattings (ToDo maybe change and add them back in)    
        
    Codes that are not part of the official hierarchy (starting with IPCM or CATM) 
    are not converted but returned without the 'CAT' or 'IPC' prefix unless the conversion is
    explicitly defined in the code_mapping dict defined in convert_IPCC_code_PRIMAP_to_pyCPA().
    
    Parameters
    ----------
    
    data_frame : scmdata.run.ScmRun
        data_frame with IPCC category codes in in PRIMAP format (IPCC1996 and IPCC2006 can be converted)
            
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        ScmRun with the converted data

        
    """
    
    all_codes = data_frame.get_unique_meta('category')
    
    replacements = dict()
    
    for code in all_codes:
        replacements[code] = convert_IPCC_code_PRIMAP_to_pyCPA(code)
    
    # make the replacement
    data_frame['category'] = data_frame['category'].replace(replacements)


########
#### function to add GWP column from variable names
########
def add_GWP_information(data_frame, defaultGWP):
    """
    This function takes GWP information from the variable name (if present) and uses the default given otherwise.
    The default is given as "AR4", "SAR", "AR5" etc (as in the unit names). GWP information is stored in the
    metadata column "unit_context"
    
    Attention: the function has to be used before "convert_unit_PRIMAP_to_scmdata"
    
    Currently the fucntion uses a limited set of GWP values (SAR, AR4, AR5) and works on a limited 
    set of variables (KYOTOGHG, HFCS, PFCS, FGASES).
    
    TODO: integrate check for converted units (or integrate this function in the unit conversion)
    
    Parameters
    ----------
    
    data_frame : scmdata.run.ScmRun
        data_frame with IPCC category codes in in PRIMAP format (IPCC1996 and IPCC2006 can be converted)
    
    defaultGWP : string
        Default GWP value to be used if no GWP can be inferred from the variable name
        
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        ScmRun with the converted data
        
    """
    
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
        data_frame["unit_context"] = data_frame["variable"] # + ' ' + defaultGWP
        
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
        data_frame['variable'] = data_frame['variable'].replace(variable_replacements)
        data_frame['unit_context'] = data_frame['unit_context'].replace(GWP_replacements)


########
#### function to bring units in scmdata format
########
def convert_unit_PRIMAP_to_scmdata(data_frame):
    """
    This function converts the emissions module style units which usually neither carry information about the 
    substance nor about the time to pyCPA/openscm units. The function also handles the exeption cases where
    PRIMAP units do contain information about the substance (e.g. GtC).
    
    The function operates in a ScmRun dataframe which is alteed in place.
    
    Parameters
    ----------
    
    data_frame : scmdata.run.ScmRun
        data_frame with IPCC category codes in in PRIMAP format (IPCC1996 and IPCC2006 can be converted)
    
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        ScmRun with the converted data
        
    """
    
    
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
    data_frame["variable_unit"] = data_frame["unit"].astype(str) + ' ' + data_frame["variable"].astype(str) + time_frame_str
    
    # get unique units
    #unit_values = data_frame.get_unique_meta('unit')
    #present_exception_units = []
    replacements = dict()
    for exception_unit in list(exception_units.keys()):
        #print(exception_unit)
        # TODO this needs error handling (e.g. when nothing has been matched)
        CRF_current_unit = data_frame.filter(unit = regexp_str + exception_unit + '$', regexp=True,
                                             log_if_empty = False)
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
    
    data_frame['variable_unit'] = data_frame['variable_unit'].replace(replacements)
    data_frame['unit'] = data_frame['variable_unit']
    data_frame.drop_meta({'variable_unit'}, inplace = True)
    

########    
#### function to combine data for different values of metadata
########
def combine_rows(data_frame, mapping, other_cols, cols_to_remove: list = [], inplace: bool = True, 
                 verbose: bool = False) -> scmdata.run.ScmRun:
    """
    This function combines rows for given values of a meta data columns where all other values coincide 
    with other_cols (a dict) a subset of the dataset can be defined which the function operates on. 
    operator will be used for all but the first values_to_combine (first always has positive sign)
    
    # currently only add and substract operators are implemented
    # TODO iplement checks for operator

    Parameters
    ----------    
    data_frame : scmdata.run.ScmRun
        data_frame to work on
        
    mapping : dict
        a dict which has column names as keys. Each entry is a list where the first item is a 
        list of values for the column (the values to be mapped), the second item is the operator 
        (or list of operators), and the third item is the target value
               
    other_cols : dict
        a dict with column: value pairs specifying to only work on a subset of the dataframe 
        defined by the column: value pairs
        
    cols_to_remove : list
        list with column names that should be removed from the result dataframe. Default: empty list
        
    inplace : bool
        bool defining if operation takes place on input DF (True) or if newly generated data will be returned
        as an individual DF while the input DF remains unchanged (False). Default: True
    
    verbose : bool
        bool. If True a lot of debug information is written to the terminal. Default: False 
    
    Returns
    -------
    
    :obj:`scmdata.run.ScmRun`
        ScmRun with the mapped data. Only the newly generated data will be returned
        
    Examples
    --------
    
    *Example for mapping*::
        
        mapping = {
            "category": [['1.A', '1.B', '1.C'], '+', '1'],
            "type_t": [['ACT'], ['+'], 'NET'],
            }
    
    The example would combine categories 1.A, 1.B, and 1.C for type ACT to category 1, type NET
    Operators can be used on all columns but be careful with using them on more than one column.
    Tey will be applied after each other, so if a time series gets a '-' operator from two columns
    the result will be '+'.    
    
    """    
          
    # first filter the dataset such that we only work on the data defined by other_cols and other_col_values
    # we also don't need the rows which have vales of "column" which are not in values_to_combine
    col_filter = other_cols.copy()
    for column in mapping.keys():
        if inplace: 
            values_to_combine_and_target = mapping[column][0].copy()
            values_to_combine_and_target.append(mapping[column][2])
            col_filter[column] = values_to_combine_and_target
        else:
            col_filter[column] = mapping[column][0]
        
    if verbose:
        print('col_filter = ')
        print(col_filter)   
    data_to_work_with = data_frame.filter(keep = True, inplace = False, **col_filter, log_if_empty = False)
    
    if data_to_work_with.timeseries().empty:
        print('No data left after filtering for wanted column values: filter: ')
        for col in col_filter.keys():
            print(col + ': ' + ', '.join(col_filter[col]))
        return
    
    # remove this data from the main data_frame. The current approch saves memory but looses data if function crashes.
    # one could also work on a copy and return the copy only if all worked out
    # don't remove as we might want additional rows not replacement of rows
    # data_frame.filter(keep = False, inplace = True, **col_filter)

    # now split existing result time series from rest of data_to_work_with
    if not inplace:
        col_filter = {}
        for column in mapping.keys():
            col_filter[column] = mapping[column][2]
        existing_result_timeseries = data_to_work_with.filter(keep = True, inplace = False, 
                                                              **col_filter, log_if_empty = False)
        # check if result timeseries already exists (currently not used)
        if existing_result_timeseries.timeseries().empty:
            result_existing = False
        else:
            result_existing = True
            
    # get the data to combine
    col_filter = {}
    #zero_cols = {}
    for column in mapping.keys():
#        if mapping[column][0] == '\\ZERO':
#            to_zero = True
#            #zero_cols[column] = mapping[column][2]
#        else:
#            to_zero = False
#            col_filter[column] = mapping[column][0]
        col_filter[column] = mapping[column][0]
    data_to_work_with.filter(keep = True, inplace = True, **col_filter, log_if_empty = False)
    #if verbose:
    #    print(data_to_work_with.head())
       
#    if to_zero:
#        # create zero rows for all combinations of metadata values (except the cols operated on)
#        
#        # get all combinations of metadata values
#        all_columns = data_to_work_with.meta.columns.values
#        ignore_cols = ['unit', 'unit_context']
#        relevant_cols = list(set(all_columns) - set(mapping.keys()) - )
#        group_col_combinations = data_this_var.meta[group_cols]
#        unique_GCC = group_col_combinations.drop_duplicates()
    
    # check if all units are the same
    all_units = data_to_work_with.get_unique_meta('unit')
    if len(all_units) > 1:
        # we need unit conversion
        if verbose:
            print('unit conversion needed. units are ' + ', '.join(all_units))
        # check if all variables are the same
        all_vars = data_to_work_with.get_unique_meta('variable')
        if len(all_vars) > 1:
            # now we have two cases: 
            # 1) the variable is operated on and we need CO2 equivalence
            # 2) the variable is a group column and we need unit conversion per combination of 
            #    group col values
            CO2eq_needed = False
            if 'variable' in mapping.keys():
                if len(mapping['variable'][0]) > 1:
                    CO2eq_needed = True
                    # theoretically we also have to check if the result entity is  CO2eq entity
                    # it doesn't make much sense to just convert CO2 to KYOTOGHG but just in case
                    # somebody does it we should add the code for that here
            
            if CO2eq_needed:
                if verbose:
                    print('Need CO2 equivalence as variables are combined')
                # 1) CO2 equivalence is needed
                unit_to = 'Mt CO2 / yr'
                # get all contexts present
                all_contexts = data_to_work_with.get_unique_meta('unit_context')

                for current_context in all_contexts:
                    # convert data
                    if verbose:
                        print('converting for context ' + current_context)
                        print('unit_to: ' + unit_to)
                    data_to_work_with = data_to_work_with.convert_unit(unit_to, context = current_context, inplace = False, 
                                                  **{'unit_context': current_context})
                    #print(data_to_work_with.head())
                    
            else:
                # 2) no CO2 equivalence but individual conversion for each variable
                for variable in all_vars:
                    if verbose:
                        print('converting for variable ' + variable) 
                    data_this_var = data_to_work_with.filter(keep = True, inplace = False, 
                                                             **{'variable': variable}, log_if_empty = False)
                    all_units_this_var = data_this_var.get_unique_meta('unit')
                    if verbose:
                        print('All units for ' + variable + ': ' + ', '.join(all_units_this_var))
                    if len(all_units_this_var) > 1:
                        # now we have to check the other metadata cols which are not combined
                        all_columns = data_this_var.meta.columns.values
                        all_columns_to_check = list(set(all_columns) - set(mapping.keys()))
                        
                        group_cols = []
                        unique_cols = dict()

                        for current_column in all_columns_to_check:
                            values_this_col = data_to_work_with.get_unique_meta(current_column)
                            if len(values_this_col) > 1:
                                group_cols.append(current_column)
                            else:
                                unique_cols[current_column] = values_this_col[0]
                        
                        # remove unit from group cols. If it's not in the list an exeption will 
                        # be raised. This is fine as it should exist unless there is a problem 
                        # with the code
                        group_cols.remove('unit')
                        
                        if group_cols:
                            # we have more columns that are not mapped and might be 
                            # responsible for the different units. Check that so we 
                            # don't have to convert where it's not necessary or not 
                            # possible. We get all possible combinations and work on
                            # tme one by one
                            if verbose:
                                print('Checking unit conversion for combinations of cols ' + 
                                      ', '.join(group_cols))
                                
                            group_col_combinations = data_this_var.meta[group_cols]
                            unique_GCC = group_col_combinations.drop_duplicates()
                            
                            for iRow in range(len(unique_GCC)):
                                value_list_GCC = list(unique_GCC.iloc[iRow])
                                if verbose:
                                    print('Value combination :' + ', '.join(value_list_GCC))
                                # build filter
                                filter_GCC = dict(zip(group_cols, value_list_GCC))
                                                                
                                # check if we have several units
                                data_this_GCC = data_this_var.filter(keep = True, inplace = False, 
                                                             **filter_GCC, log_if_empty = False)
                                all_units_this_GCC = data_this_GCC.get_unique_meta('unit')
                                if len(all_units_this_GCC) > 1:
                                    # need unit conversion
                                    unit_to = all_units_this_GCC[0]
                                    if verbose:
                                        print('converting to ' + unit_to)
                                    
                                    filter_GCC['variable'] = variable
                                    # get all contexts
                                    current_contexts = data_this_GCC.get_unique_meta('unit_context')
                                    for context in current_contexts:
                                        filter_GCC['unit_context'] = context
                                        data_to_work_with = data_to_work_with.convert_unit(unit_to, inplace = False, 
                                                                                           context = context, **filter_GCC)
                                else:
                                    if verbose:
                                        print('No conversion needed')
                        else:
                            unit_to = all_units_this_var[0]
                            if verbose:
                                print('converting to ' + unit_to)
                            # convert data
                            # get all contexts
                            current_contexts = data_this_var.get_unique_meta('unit_context')
                            for context in current_contexts:
                                data_to_work_with = data_to_work_with.convert_unit(unit_to, inplace = False, context = context, 
                                                               **{'variable': variable, 'unit_context': context})
                    elif verbose:
                        print('No conversion needed')
                    
        else: # not tested so far
            unit_to = all_units[0]
            # convert data
            # get all contexts
            current_contexts = data_to_work_with.get_unique_meta('unit_context')
            for context in current_contexts:
                data_to_work_with = data_to_work_with.convert_unit(unit_to, inplace = False, context = context, 
                                                                   **{'unit_context': context})
                
    # prepare data in case of subtractions
    for column in mapping.keys():
        # first check if we have individual operators for all values_to_combine
        operator = mapping[column][1]
        values_to_combine = mapping[column][0]
        if not values_to_combine == ['*']:
            if type(operator) is list: 
                # check if list has the correct length (move to beginning of function)
                # also add a check if it's just '+' and '-'
                if len(operator) != len(values_to_combine):
                    # throw error
                    # TODO: throw error
                    print('operator list and values_to_combine have different lengths')
                    print(operator)
                    print(mapping[column][0])
                    return
                else:
                    # find the entries with '-'
                    subtract_values = []
                    for i_operator in range(0, len(operator)):
                        if operator[i_operator] == '-':
                            subtract_values.append(values_to_combine[i_operator])
            else:
                # if only one value_to_combine apply operator to that.
                # if more than one value, apply to all but the first
                if operator == '-':
                    if len(values_to_combine) == 1:
                        subtract_values = values_to_combine
                    else:
                        subtract_values = values_to_combine[1 :]
                else:
                    subtract_values = []
    
            # now apply - to all rows with column values in subtract_values (if there are any)
            if subtract_values:
                if verbose:
                    print(subtract_values)
            
                data_to_change_sign = data_to_work_with.filter(inplace = False, keep = True, 
                                                               **{column: subtract_values}, log_if_empty = False)
                data_to_work_with.filter(inplace = True, keep = False, **{column: subtract_values}, 
                                         log_if_empty = False)
        
                data_to_change_sign_TS = data_to_change_sign.timeseries().apply(lambda x: - x)
                data_changed_sign = scmdata.run.ScmRun(data_to_change_sign_TS)
        
                data_to_work_with.append(data_changed_sign, inplace = True)
   
            
    # use groupby to create the mapped data 
    ## first find all cols that have non-unique values (except for the colums to operate on)
    all_columns = data_to_work_with.meta.columns.values
    all_columns_to_check = list(set(all_columns) - set(mapping.keys()))

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
    
    
    # sanity check if there are metadata combinations which only differ in unit
    # too much work to check here. Check result instead (not implemented yet)
#    if 'unit' in group_cols:
#        # this is not covering all cases. In general time series could differ in unit and another field 
#        # but not variable. It's just not very likely
#        if 'variable' in group_cols:
#            all_variables = data_to_work_with.get_unique_meta('variable')
#            for variable in all_variables:
#                data_this_var = data_to_work_with.filter(keep = True, inplace = False, **{'variable': variable})
#                all_units_this_var = data_this_var.get_unique_meta('unit')
#                if len(all_units_this_var) > 1:
#                    # throw error
#                    # TODO: throw error
#                    print('Unit col has non-unique metadata for variable ' + variable + 
#                          '. Something went wrong with unit conversion.')
#                    return
#        else:
#            # throw error
#            # TODO: throw error
#            print('Unit col has non-unique metadata. Something went wrong with unit conversion.')
#            return
        
    
    if verbose:
        units = data_to_work_with.get_unique_meta('unit')
        print('unit values:', *units, sep= ', ' )
        print('mapping on columns:', *group_cols, sep = ', ')
    
    # apply the operation (currently only sum)
    result_DF = data_to_work_with.timeseries().groupby(group_cols).sum()

    ## add the meta columns back in. they were removed by groupby 
    ## get all col values
    columns = data_to_work_with.meta.columns.values
    columns = list(set(columns) - set(group_cols) - set(mapping.keys()) - set(cols_to_remove))
    if verbose:
        print(columns)
    for i_column in range(0, len(columns)):

        values_this_col = data_to_work_with.get_unique_meta(columns[i_column])
        if verbose:
            print(columns[i_column])
            print(values_this_col)
        if len(values_this_col) > 1:
            # TODO: throw exception
            result_DF.insert(i_column, columns[i_column], values_this_col[0])
        else:
            result_DF.insert(i_column, columns[i_column], values_this_col[0])

    i = 0
    for column in mapping.keys():
        result_DF.insert(len(columns) + i, column, mapping[column][2])
        i += 1
        if verbose:
            print('inserting column "' + column + '" with value "' + mapping[column][2] + '"')

    # make a new ScmDataFrame 
    result_scmdata = scmdata.run.ScmRun(result_DF)

    # append to data_frame
    if inplace:
        # remove existing data before appending
        
        # check which new data has been added (unit and unit_context may have changed, so we overwrite
        # remove rows which differ from the new data in unit and unit_contect)
        col_filter = {}
        meta_to_consider = list(set(result_scmdata.meta.columns.values) - set(['unit', 'unit_context']))
        unique_meta = []
        for meta in meta_to_consider:
            values = result_scmdata.get_unique_meta(meta)
            if len(values) == 1:
                col_filter[meta] = values
                unique_meta.append(meta)
        meta_to_consider = list(set(meta_to_consider) - set(unique_meta))
        
        for iRow, row  in result_DF.iterrows():
            # set up the filter
            col_filter_current = col_filter.copy()
            for meta in meta_to_consider:
                col_filter_current[meta] = row[meta]
            
            # remove the row from the original DF
            data_frame.filter(keep = False, inplace = True, **col_filter, log_if_empty = False)
               
        data_frame.append(result_scmdata, inplace = True)
    else:
        return result_scmdata