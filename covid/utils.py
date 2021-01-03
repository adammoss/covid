import pandas as pd
import numpy as np
from functools import reduce
import os


def get_covid_activity():
    base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/01/'
    filename = 'COVID-19-daily-admissions-and-beds-20210103.xlsx'
    data = [
        {'metric': 'hospitalCases', 'first_row': 88, 'first_col': 1},
        {'metric': 'covidOccupiedMVBeds', 'first_row': 103, 'first_col': 1}
    ]
    dfs = []
    for d in data:
        df = pd.read_excel(os.path.join(base_url, filename), engine='openpyxl', skiprows=d['first_row'], nrows=8,
                           usecols=np.arange(d['first_col'], 1000))
        df = df.set_index('Name').T
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        df = df.rename_axis(None, axis=1).rename_axis('id', axis=0)
        df = pd.melt(df, id_vars=['date'], var_name='areaName', value_name=d['metric'])
        dfs.append(df)
    return reduce(lambda left, right: pd.merge(left, right, on=['date', 'areaName']), dfs)
