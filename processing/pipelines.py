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