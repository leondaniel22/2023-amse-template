import pandas as pd 
import dateparser
import sqlalchemy
from pandas import DataFrame
import os
from meteostat import Point, Daily
from datetime import datetime


def read_csv():
    """ This function reads a csv file and turns it into a pandas dataframe """

    return pd.read_csv( \
        'https://offenedaten-konstanz.de/sites/default/files/Zaehlstelle_Herose_2021_15min.csv', \
        sep = ','
    )


def read_from_meteostat():
    """ This function reads a dataset about weather conditions in the german city Konstanz (2021) and turns it into a pandas dataframe """

    # Use the metestat library to get data
    start = datetime(2021, 1, 1)
    end = datetime(2021, 12, 31)

    # Create Point for city of constance
    constance = Point(47.6833, 9.1833)

    # Get daily data for 2021
    data = Daily(constance, start, end)
    data = data.fetch()

    # put the data in a dataframe and reset the index
    df = pd.DataFrame(data)
    df = df.reset_index()

    return df


def drop_none_rows(df: DataFrame):
    """ This function drops all NaN rows of a pandas datframe """

    return df.dropna(axis=1, how='all')

def rename_columns(df: DataFrame, names: list[str]):
    if len(df.columns.values) == len(names):
        for i in range(len(names)):
            df.columns.values[i] = names[i]
    return df


def normalise_datetime(df: DataFrame):
    """ This functions normalises the date format in a pandas dataframe """
    # use a dateparser to change the time column date time to datetime and then normalize it so only the date remains 
    df['Date'] = df['Date'].apply(lambda x: dateparser.parse(x))
    df['Date'] = df['Date'].dt.normalize()
    return df


def aggregate_to_day(df: DataFrame):
    """ This function groups a pandas dataframe with a 'Date' column by days """

    # group all data of the the day and aggregate the values of the other columns
    agg_functions = {'Total bikers': 'sum', 'Bikers inward': 'sum', 'Bikers outward': 'sum'}
    df = df.groupby(df['Date'], as_index=False).aggregate(agg_functions)
    return df

    return dict(zip(meta_data, sql_dtypes))


def get_dtypes(df: DataFrame): 
    """ This function gets the correct corresponding sqlalchemy datatypes for each column """
    dtypes = {}
    for name in list(df):
        if name == "Date":
            dtypes[name] = sqlalchemy.DATE()
        else: 
            dtypes[name] = sqlalchemy.FLOAT()

    return dtypes


def save_in_db(df: DataFrame, db_name: str, table_name: str, dtype):
    """ This function saves a dataframe into an sqlite database in the folder 'data' """
    df.to_sql(table_name, 'sqlite:///data/' + db_name, if_exists='replace', index=False, dtype=dtype)


def execute_pipeline():
    """ This function executes the complete data pipeline for the following datasets:
            1. Dataset1: Data about bikers in Konstanz
            2. Dataset2: Data about weather conditions in Konstanz
    """

    # the name of the database to store the datasets
    db_name = "datasets.sqlite"

    # ---- Dataset1 ----
    df1 = read_csv()
    df1 = drop_none_rows(df1) 
    df1_names = ["Date", "Total bikers", "Bikers inward", "Bikers outward"]
    df1 = rename_columns(df1, names=df1_names)
    df1 = normalise_datetime(df1)
    df1 = aggregate_to_day(df1)
    dtype = get_dtypes(df1)
    save_in_db(df1, db_name=db_name, table_name="dataset1", dtype=dtype)

    # ---- Dataset2 ----
    df2 = read_from_meteostat()
    df2_names = ["Date", "Average temperature", "Min temperature", "Max temperature", 
                 "Total rainfall", "Snow-level", "Wind-direction", "Wind-speed", 
                 "Lace boe", "Air pressure", "Duration of sunshine"]
    df2 = rename_columns(df2, names=df2_names)
    dtype = get_dtypes(df2)
    save_in_db(df2, db_name=db_name, table_name="dataset2", dtype=dtype)

execute_pipeline()







