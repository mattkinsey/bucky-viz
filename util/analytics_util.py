import pathlib
import time
import pandas as pd
import os
from os import path

from datetime import datetime
from datetime import timedelta

import sys

arbitrary_run_date = datetime(2022, 7, 12)


today = datetime.now().date()
today_str = str(today)
today_dt = pd.to_datetime(today)


in_string = str(today)
f = "%Y-%m-%d"  # YEAR - MONTH - DAY    YYYY-MM-DD
in_object = datetime.strptime(in_string, f)


def nearest_prior_monday_date(given_date):  # TAKES datetime.date(YYYY, MM, DD)
    idx = (given_date.weekday() + 7) % 7
    return given_date - timedelta(idx)


nearest_prior_monday = nearest_prior_monday_date(in_object)
monday_str = str(nearest_prior_monday.date())


def nearest_prior_thursday_date(given_date):  # TAKES datetime.date(YYYY, MM, DD)
    idx = (given_date.weekday() + 4) % 7

    return given_date - timedelta(idx)


nearest_prior_thursday = nearest_prior_thursday_date(in_object)
thursday_str = str(nearest_prior_thursday.date())


def nearest_prior_sunday_date(given_date):  # TAKES datetime.date(YYYY, MM, DD)
    idx = (given_date.weekday() + 1) % 7
    return given_date - timedelta(idx)


nearest_prior_sunday = nearest_prior_sunday_date(in_object)
sunday_str = str(nearest_prior_sunday.date())


def f_comma(number):
    return "{:,.0f}".format(number)

#f_comma(100000000)  # usage


def get_creation(cpath):
    ti_c = os.path.getctime(cpath)
    c_ti = time.ctime(ti_c)
    return c_ti


run_date = "run_date_" + today_str

user_path = pathlib.Path.home()



def unzip_most_recent_bucky_output_targz_files(path, date_str):
    # find filenames
    # unzip
    # return file path list
    import tarfile
    
    print('targz path',path)
    print('targz date_str',date_str)
    
    # get file names
    files_list = []
    for root, directories, files in os.walk(path):
        for f in files:
            if date_str is not None:
                if date_str in f:
                    files_list.append(os.path.join(root, f))

    folders_list = []
    for root, directories, files in os.walk(path):
        for d in directories:
            if date_str is None:
                folders_list.append(d)
            else:
                if date_str in d:
                    folders_list.append(d)

    # unzip
    for i in files_list:
        print('look for unzipped files',i)
        p = i # path + i
        if date_str in i:
            print('unzipping',i)
            file = tarfile.open(p)
            file.extractall(path)
            file.close()

    # get folder names
    folders_list = []
    for root, directories, files in os.walk(path):
        for d in directories:
            if date_str is None:
                folders_list.append(d)
            else:
                print('date_str',date_str)
                if date_str in d:
                    #print(d,'----')
                    folders_list.append(d)

    print('done.',folders_list)
    return folders_list


def get_most_recent_bucky_output_folders(path, date_str):
    # find filenames
    # unzip
    # return file path list
    import tarfile
    print('Loading local data...')
    print('get bucky: ', path, date_str)
    folders_list = []
    for root, directories, files in os.walk(path):
        for d in directories:
            if date_str is None:
                folders_list.append(d)
            else:
                if date_str in d:
                    folders_list.append(d)

    print('Models found.',folders_list)
    return folders_list



#files_list = get_most_recent_bucky_output_targz_files('path','date_str')

def get_bucky_output_df(data_path, path):
    import pandas as pd
    model_name = path.split('/')[-1]
    path = data_path + path + '/adm0_quantiles.csv'
    #print('path',path)
    df = pd.read_csv(path)
    df = df[['date','adm0','quantile','daily_reported_cases','daily_deaths','daily_hospitalizations']]
    df['model'] = model_name
    df['adm1'] = '-1'
    #df.columns = ['date','adm1','quantile','daily_reported_cases','daily_deaths','daily_hospitalizations','model']
    return df

def get_bucky_output_1_df(data_path, path):
    import pandas as pd
    model_name = path.split('/')[-1]
    path = data_path + path + '/adm1_quantiles.csv'
    #print('path',path)
    df = pd.read_csv(path)
    df = df[['date','adm1','quantile','daily_reported_cases','daily_deaths','daily_hospitalizations']]
    df['model'] = model_name
    #df.columns = ['date','adm1','quantile','daily_reported_cases','daily_deaths','daily_hospitalizations','model']
    return df


def show_paths():
    print('today: ', today_str)
    print('prior monday: ', monday_str)
    #print('prior thursday: ', thursday_str)
    #print('prior sunday: ', sunday_str)

    return True
