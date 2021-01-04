import pandas as pd
import numpy as np
from functools import reduce
import os
import datetime


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


def get_uec_sitrep(year):
    if year == '202021':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/12/'
        filename = 'UEC-Daily-SitRep-Acute-Web-File-Timeseries-5.xlsx'
        occupancy_fraction = False
    elif year == '201920':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2020/03/'
        filename = 'Winter-SitRep-Acute-Time-series-2-December-2019-1-March-2020.xlsx'
        occupancy_fraction = True
    elif year == '201819':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2019/03/'
        filename = 'Winter-data-timeseries-20190307.xlsx'
        occupancy_fraction = True
    elif year == '201718':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2018/03/'
        filename = 'Winter-data-Timeseries-20180304.xlsx'
        occupancy_fraction = True
    elif year == '201617':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2016/12/'
        filename = 'DailySR-Web-file-Time-Series-18.xlsx'
        occupancy_fraction = False
    elif year == '201516':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2015/12/'
        filename = 'DailySR-Timeseries-WE-28.02.16.xlsx'
        occupancy_fraction = False
    elif year == '201415':
        base_url = 'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2014/12/'
        filename = 'DailySR-Timeseries-WE-29.03.15.xlsx'
        occupancy_fraction = False
    df = pd.read_excel(os.path.join(base_url, filename), engine='openpyxl', skiprows=13, nrows=1000,
                       sheet_name='Adult critical care', usecols=[1, 3] + np.arange(4, 1000).tolist())
    df.dropna(how='all', inplace=True)
    df.drop(index=0, inplace=True)
    df = df.rename(columns={'Unnamed: 1': 'areaName', 'Unnamed: 3': 'code', 'Unnamed: 4': 'trust'})
    df = df[df['code'].notna()]
    df = df[df['code'] != '-']
    column_indexes = []
    for i, c in enumerate(df.columns):
        if isinstance(c, datetime.datetime):
            column_indexes.append(i)
    column_indexes = np.array(column_indexes)
    df_available = df.drop(columns=df.columns[column_indexes + 1])
    if occupancy_fraction:
        df_available = df_available.drop(columns=df.columns[column_indexes + 2])
    df_occupied = df.drop(columns=df.columns[column_indexes])
    if occupancy_fraction:
        df_occupied = df_occupied.drop(columns=df.columns[column_indexes + 2])
    df_occupied.columns = df_available.columns
    df_available = pd.melt(df_available, id_vars=['areaName', 'code', 'trust'], var_name='date',
                           value_name='availableMVBeds')
    df_occupied = pd.melt(df_occupied, id_vars=['areaName', 'code', 'trust'], var_name='date',
                          value_name='occupiedMVBeds')
    df = pd.merge(df_available, df_occupied, on=['date', 'code', 'areaName', 'trust'])
    df = df.replace('-', np.nan)
    return df
