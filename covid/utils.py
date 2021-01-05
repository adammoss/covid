import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from functools import reduce
import os
import datetime


def get_covid_activity():
    url = 'https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-hospital-activity/'
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content, 'html.parser')
    file_url = None
    for link in soup.find_all('a'):
        if 'xlsx' in link['href'] and 'daily' in link['href']:
            file_url = link['href']
    assert file_url is not None
    data = [
        {'metric': 'hospitalCases', 'first_row': 88, 'first_col': 1},
        {'metric': 'covidOccupiedMVBeds', 'first_row': 103, 'first_col': 1}
    ]
    dfs = []
    for d in data:
        df = pd.read_excel(file_url, engine='openpyxl', skiprows=d['first_row'], nrows=8,
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


def get_ons_deaths(year):
    assert year in range(2010, 2021)
    date_row = 0
    if year == 2020:
        filename = 'publishedweek512020corrected.xlsx'
        skiprows = 4
        total_row = 3
        region_row_min = 80
        region_row_max = 91
        engine = 'openpyxl'
        ini_columns = 2
    elif year == 2019:
        filename = 'publishedweek522019.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 2
    elif year == 2018:
        filename = 'publishedweek522018withupdatedrespiratoryrow.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 2
    elif year == 2017:
        filename = 'publishedweek522017.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 2
    elif year == 2016:
        filename = 'publishedweek522016.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 2
    elif year == 2015:
        filename = 'publishedweek2015.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 1
    elif year == 2014:
        filename = 'publishedweek2014.xls'
        skiprows = 2
        total_row = 3
        region_row_min = 38
        region_row_max = 49
        engine = 'xlrd'
        ini_columns = 1
    elif year == 2013:
        filename = 'publishedweek2013.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 1
    elif year == 2012:
        filename = 'publishedweek2012.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 1
    elif year == 2011:
        filename = 'publishedweek2011.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 38
        region_row_max = 49
        engine = 'xlrd'
        ini_columns = 1
    elif year == 2010:
        filename = 'publishedweek2010.xls'
        skiprows = 3
        total_row = 3
        region_row_min = 37
        region_row_max = 48
        engine = 'xlrd'
        ini_columns = 1
    userows = []
    for i in range(1000):
        if i == date_row or i == total_row or region_row_min < i < region_row_max:
            userows.append(i)
    try:
        df = pd.read_excel('../data/all_cause_mortality/' + filename, engine=engine,
                           sheet_name='Weekly figures ' + str(year), skiprows=skiprows)
    except:
        df = pd.read_excel('../data/all_cause_mortality/' + filename, engine=engine,
                           sheet_name='Weekly Figures ' + str(year), skiprows=skiprows)
    df.dropna(how='all', inplace=True)
    for i, row in df.iterrows():
        if i not in userows:
            df.drop(index=i, inplace=True)
    if ini_columns == 1:
        df = df.rename(columns={'Week number': 'areaName'})
        df.at[3, 'areaName'] = 'Total'
    elif ini_columns == 2:
        df = df.rename(columns={'Unnamed: 1': 'areaName'})
        df.at[3, 'areaName'] = 'Total'
        df.drop(columns='Week number', inplace=True)
    df.at[0, 'areaName'] = 'date'
    df = df.set_index('areaName').T
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'week'}, inplace=True)
    df = df.rename_axis(None, axis=1).rename_axis('id', axis=0)
    df = pd.melt(df, id_vars=['week', 'date'], var_name='areaName', value_name='totalDeaths')
    df["totalDeaths"] = pd.to_numeric(df["totalDeaths"], errors='coerce')
    return df
