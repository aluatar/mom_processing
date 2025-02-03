from pandas import (
    DataFrame
)
from numpy import (
    array,
    pi,
    unique,
    abs,
)

from .utils import (
    import_data,
    separate_characteristics_from_params,
    get_orbital_numbers,
    get_parities,
)

from abc import (
    ABC,
    abstractmethod
)





class Pipeline(ABC):
    pass
    



def pipeline_modes_vs_h(path: str) -> dict[int, dict[str, array]]:
    number_of_parameters = 1
    M_0 = 140 * 1e3 #[A/m]
    gamma = 2.21e5 #
    giga = 1e9

    df = import_data(path=path)
    df = separate_characteristics_from_params(
        dataframe=df,
        number_of_params=number_of_parameters)
    df = get_orbital_numbers(dataframe=df)
    columns = df.columns

    unique_ms = unique(df['m'])
    mode_m_map = {}
    for m in unique_ms:
        h = array(df.loc[df['m'] == m][columns[0]])
        freq = array(df.loc[df['m'] == m][columns[1]])

        mode_m_map[m] = {
            'freq': (2 * pi * freq / gamma - h) / (4 * pi * M_0),
            'h': h / (4 * pi * M_0)
        }
        
    return mode_m_map


def pipeline_modes_vs_r(path: str) -> dict[int, dict[str, array]]:
    number_of_parameters = 1
    M_0 = 140 * 1e3 #[A/m]
    gamma = 2.21e5 #
    giga = 1e9

    df = import_data(path=path)
    df = separate_characteristics_from_params(
        dataframe=df,
        number_of_params=number_of_parameters)
    df = get_orbital_numbers(dataframe=df)
    columns = df.columns

    unique_ms = unique(df['m'])
    mode_m_map = {}
    for m in unique_ms:
        r = array(df.loc[df['m'] == m][columns[0]])
        freq = array(df.loc[df['m'] == m][columns[1]])

        mode_m_map[m] = {
            'freq': freq,
            'r': r
        }
        
    return mode_m_map


def pipeline_modes_vs_s(path: str) -> dict[int, dict[str, array]]:
    number_of_parameters = 1
    M_0 = 140 * 1e3 #[A/m]
    gamma = 2.21e5 #
    giga = 1e9

    df = import_data(path=path)
    df = separate_characteristics_from_params(
        dataframe=df,
        number_of_params=number_of_parameters)
    df = get_orbital_numbers(dataframe=df)
    columns = df.columns

    unique_ms = unique(df['m'])
    mode_m_map = {}
    for m in unique_ms:
        r = array(df.loc[df['m'] == m][columns[0]])
        freq = array(df.loc[df['m'] == m][columns[1]])

        mode_m_map[m] = {
            'freq': freq,
            's': r
        }
        
    return df


def pipeline_modes_vs_formfactor_parity(path: str):
    import os, pandas
    number_of_parameters = 1
    df_list = []
    for file in os.listdir(path):
        if file.endswith('.csv'):
            full_path = path + '/' + str(file)
            df = import_data(path=full_path)
            df = separate_characteristics_from_params(
                    dataframe=df,
                    number_of_params=number_of_parameters
                )
            df_list.append(df)

    if len(df_list) > 1:
        result = pandas.concat(df_list, ignore_index=False)
        result = result.reset_index(drop=True)

    elif len(df_list) == 1:
        result = df_list[0]
    else:
        raise IndexError
    
    result = get_orbital_numbers(dataframe=result, number_of_parameters=1)
    result['index'] = result.index

    result = result.rename(columns={'m': 'p'})
    result['p'] = [p if p == 1 else -1 for p in result['p']]
    result['ef_1'] = abs(result['ef_1'])
    
    result = result.sort_values(by=['index'])
    result = result.reset_index(drop=True)

    return result


def pipeline_modes_vs_formfactor_m(path: str):
    import os, pandas
    number_of_parameters = 1
    df_list = []
    for file in os.listdir(path):
        if file.endswith('.csv'):
            full_path = path + '/' + str(file)
            df = import_data(path=full_path)
            df = separate_characteristics_from_params(
                    dataframe=df,
                    number_of_params=number_of_parameters
                )
            df_list.append(df)

    if len(df_list) > 1:
        result = pandas.concat(df_list, ignore_index=False)
        result = result.reset_index(drop=True)

    elif len(df_list) == 1:
        result = df_list[0]
    else:
        raise IndexError
    
    result = get_orbital_numbers(dataframe=result, number_of_parameters=1)
    result['index'] = result.index

    
    result = result.sort_values(by=['index'])
    result = result.reset_index(drop=True)

    return result


def pipeline_modes_vs_formfactor_char(path: str):
    import os, pandas
    number_of_parameters = 1
    df_list = []
    for file in os.listdir(path):
        if file.endswith('.csv'):
            full_path = path + '/' + str(file)
            df = import_data(path=full_path)
            df = separate_characteristics_from_params(
                    dataframe=df,
                    number_of_params=number_of_parameters
                )
            df_list.append(df)

    if len(df_list) > 1:
        result = pandas.concat(df_list, ignore_index=False)
        result = result.reset_index(drop=True)

    elif len(df_list) == 1:
        result = df_list[0]
    else:
        raise IndexError
    
    result['index'] = result.index
    
    return result


def routine_mode_parity(path: str, number_of_parameters: int):
    import os, pandas

    df_list = []
    for file in os.listdir(path):
        if file.endswith('.csv'):
            full_path = path + '/' + str(file)
            df = import_data(path=full_path)
            df = separate_characteristics_from_params(
                    dataframe=df,
                    number_of_params=number_of_parameters
                )
            df_list.append(df)

    if len(df_list) > 1:
        result = pandas.concat(df_list, ignore_index=False)
        result = result.reset_index(drop=True)

    elif len(df_list) == 1:
        result = df_list[0]
    else:
        raise IndexError

    return result


def pipeline_modes_parities(paths: tuple[str,str,str]) -> DataFrame:
    number_of_parameters = 1
    df_xyz = [routine_mode_parity(path=path, number_of_parameters=number_of_parameters) for path in paths]
    df = df_xyz[0]
    df.loc[:,df.columns[number_of_parameters + 1]] = (
        df.loc[:,df.columns[number_of_parameters + 1]] 
        + df_xyz[1].loc[:,df.columns[number_of_parameters + 1]]
        + df_xyz[2].loc[:,df.columns[number_of_parameters + 1]]
    )

    df = get_parities(dataframe=df, number_of_parameters=number_of_parameters)
    df['index'] = df.index
    
    return df

