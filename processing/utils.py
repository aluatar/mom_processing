from pandas import (
    DataFrame,
    read_csv,
)
from numpy import (
    array,
)
from typing import Literal
from logging import Logger

logger = Logger(name=__name__)

def import_data(path: str, 
                columns_names: tuple[str] | None=None,
                valuetype: Literal['float','complex'] | None='float',
                **kwargs) -> DataFrame:
    """
    Utility for data import. 

    Parameters:
    -----------
    
    :param path: str, path to the <.csv> table with data
    :param columns_names: tuple of strings, *optional*, desired names of the columns in the scending order
    :param valuetype: str ('float' or 'complex') or null, type for values in the DataFrame. Default is 'float'.

    Return:
    -------
    :return: DataFrame with data from table
    """

    dataframe: DataFrame = read_csv(path, sep=",", comment='%', header=None)
    
    name_map = {}
    for i, column in enumerate(dataframe.columns):
        dataframe = (
            dataframe.rename(columns={column: i}) if columns_names is None 
            else dataframe.rename(columns={column: columns_names[i]}) 
        )
        try:
            dataframe[i] = dataframe[i].str.replace('i','j').astype('complex')
            name_map[i] = columns_names[i]
        except Exception as exp:
            if type(exp) == IndexError:
                raise IndexError.add_note(f"Length of the columns_names tuple is less then nomber of columns")
            else:
                logger.warning(msg=f"While hendling complex numbers exception {exp} of the type {type(exp)} occured")
                continue

    dataframe = dataframe.astype(valuetype)

    return dataframe


def separate_characteristics_from_params(
        dataframe: DataFrame,
        number_of_params: int
) -> DataFrame:
    """
    Utility for separating mode characteristics such as orbital numner *m* or mode volume *V* from 
    variable parameters and eigenfrequencies in the calculation result table by addin prefixes 'param' 
    to the names of parameter, 'ef' to h name of eienfrequency column, and 'char' to the names of characistics columns.

    Parameters:
    -----------

    :param dataframe: DataFrame, data to separate
    :param number_of_parameters: int, number from 1 to inf of parameters that were varied in COMSOL parametric sweep
    :param columns_names: tuple of strings, optional, luple of column_names

    Returns:
    --------

    :return dataframe: DataFrame, dataframe with separated parameters, eigenfrequencies, and characteristics
    """

    name_map = {}
    for i, column in enumerate(dataframe.columns):
        if i < number_of_params:
            name_map[column] = 'param_' + str(column)
        elif i == number_of_params:
            name_map[column] = 'ef_' + str(column)
        else:
            name_map[column] = 'char_' + str(column)

    dataframe = dataframe.rename(columns=name_map)
    return dataframe


def get_orbital_numbers(
        dataframe: DataFrame,
        number_of_parameters: int | None=None,
        ) -> DataFrame:
    """
    Utility for obtaining orbital number *m* from the table data.

    Parameters:
    -----------
    
    :param dataframe: DataFrame, dataframe with initial data
    :param number_of_parameters: int, optional, number from 1 to inf of parameters that were varied in COMSOL parametric sweep

    Returns:

    :return: DataFrame with orbital number *m* assined to each mode
    """

    if number_of_parameters is None:
        number_of_parameters = 0
        for column in dataframe.columns:
            if column[:5] == 'param':
                number_of_parameters += 1
            elif column[:2] == 'ef' or column[:4] == 'char':
                break
            else:
                logger.error(msg=f"Incorrect prefix. Define nymber of variable parameters explicitly or apply 'separate_characteristics_from_params' method to the dataframe")
    

    ms_of_modes = [[] for i in range(number_of_parameters + 2)]
    columns = dataframe.columns

    for row in range(dataframe.index.stop):
        dataframe.loc[row,columns[number_of_parameters+1]:] = abs(dataframe.loc[row,columns[number_of_parameters+1]:])
        for c in range(number_of_parameters+1, len(dataframe.loc[row])):

            if dataframe.loc[row,columns[c]] == max(dataframe.loc[row, columns[number_of_parameters+1]:]):
                for i in range(number_of_parameters + 1):
                    ms_of_modes[i].append(dataframe.loc[row,columns[i]])    
                ms_of_modes[number_of_parameters+1].append(c - number_of_parameters - 1)
                
    new_columns = [columns[i] for i in range(number_of_parameters + 1)]
    new_columns.append('m')
    new_dataframe = DataFrame(array(ms_of_modes).T, columns=new_columns).sort_values(by=new_columns[:-1])

    return new_dataframe