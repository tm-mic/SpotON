import numpy as np
import pandas as pd


def normalize_column(series: pd.Series, new_max=1, new_min=0) -> float:
    """
    Casts series data points between 0-1 based on min and max values in series.
    :param: date_to_norm: int or float value to normalize.
    :param: abs_min: Min val. in scope.
    :param: abs_max: Max val. in scope.
    :return: Normalized value between 0-1 of Dataframe.
    """
    abs_max = series.max()
    abs_min = series.min()
    return series.apply(lambda date: ((date - abs_min) / (abs_max - abs_min)) * (new_max - new_min) + new_min)

