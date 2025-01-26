from pandas import (
    DataFrame
)
from numpy import (
    array,
    pi,
    unique,
)

from .utils import (
    import_data,
    separate_characteristics_from_params,
    get_orbital_numbers,
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
        
    return mode_m_map


def pipeline_modes_vs_scale(path: str):
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

    result = pandas.concat(df_list, ignore_index=False)

    result = result.sort_values(by=list(result.columns[0:2]))
    result = result.reset_index(drop=True)
    result = get_orbital_numbers(dataframe=result, number_of_parameters=3)
    columns = df.columns

    parity_map ={
        0: [0,0,0],
        1: [0,0,1],
        2: [0,1,0],
        3: [1,0,0],
       4: [0,1,1],
       5: [1,0,1],
        6: [1,1,0],
        7: [1,1,1],
    }
    parities = [[],[],[]]

    for m in result['m']:
        parities[0].append(parity_map[m][0])
        parities[1].append(parity_map[m][1])
        parities[2].append(parity_map[m][2])

    print(parities)

    result['Px'] = parities[0]
    result['Py'] = parities[1]
    result['Pz'] = parities[2]
    result = result.drop(columns=['m'])

    return result
