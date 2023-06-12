import pytest
import os
import pandas as pd
import sqlite3


def get_database_connection():
    """ Helper function to connect to the database """
    directory_path = os.path.join(os.getcwd(), 'data/')
    return sqlite3.connect(os.path.join(directory_path,"datasets.sqlite"))

def get_dataframe(cnx: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    """ Helper function to get dataframes from database tables """
    return pd.read_sql_query("SELECT * FROM " + table_name, cnx)


def test_database():
    """ Test if after the execution of the pipeline, both datasets are safed in a sqlite database file in the data directory """
    # get directory path
    directory_path = os.path.join(os.getcwd(), 'data/')
    # check if sqlite file exists
    assert os.path.exists(os.path.join(directory_path,"datasets.sqlite"))


def test_table():
    """ Test if a table for dataset and dtaset 2 exists"""
    # connect to database
    cnx = get_database_connection()
    # get cursor
    c1 = cnx.cursor()
    c2 = cnx.cursor()
    # get the count of tables with the name dataset1 and dataset 2
    c1.execute(" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='dataset1'")
    c2.execute(" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='dataset2' ")
    dataset1_exists = c1.fetchone()[0]==1 # if the count is 1, then table exists
    dataset2_exists = c2.fetchone()[0]==1 # if the count is 1, then table exists
    # check if tables dataset1 and dataset2 exist
    assert dataset1_exists
    assert dataset2_exists

def test_shape():
    """ Test if the shapes of both dataframes are correct as expected"""

    # connect to database and extract dataset 1 and dataset 2 as pandas dataframes
    cnx = get_database_connection()
    df1 = get_dataframe(cnx, "dataset1")
    df2 = get_dataframe(cnx, "dataset2")

    # define expected and actual shapes of dataset 1 and dataset2
    df1_expected_shape = (365, 4)
    df2_expected_shape = (365, 11) 
    df1_actual_shape = df1.shape 
    df2_actual_shape = df2.shape

    # check if the actual shapes of the dataframes match the expected shapes
    assert len(df1_actual_shape) == 2
    assert len(df2_actual_shape) == 2 
    assert df1_expected_shape[0] == df1_actual_shape[0] 
    assert df1_expected_shape[1] == df1_actual_shape[1]
    assert df2_expected_shape[0] == df2_expected_shape[0]
    assert df2_expected_shape[1] == df2_actual_shape[1]

def test_columns():
    """ Test if the columns of both dataframes are correct as expected """

    # connect to database and extract dataset 1 and dataset 2 as pandas dataframes
    cnx = get_database_connection()
    df1 = get_dataframe(cnx, "dataset1")
    df2 = get_dataframe(cnx, "dataset2")

    # define expected and actual columns of dataset 1 and dataset2
    df1_expected_columns = ['Date', 'Total bikers', 'Bikers inward', 'Bikers outward'] 
    df2_expected_columns = ['Date', 'Average temperature', 'Min. temperature', 'Max. temperature',
       'Total rainfall', 'Snow-level', 'Wind-direction', 'Wind-speed',
       'Lace boe', 'Air pressure', 'Duration of sunshine'] 
    df1_actual_columns = df1.columns 
    df2_actual_columns = df2.columns 

    # check if the actual columns of the dataframes match the expected columns
    assert len(df1_actual_columns) == len(df1_expected_columns)
    assert all([a == b for a, b in zip(df1_actual_columns, df1_expected_columns)])
    assert len(df2_actual_columns) == len(df2_expected_columns)
    assert all([a == b for a, b in zip(df2_actual_columns, df2_expected_columns)])


def test_pipeline():
    """ Test if the pipeline script works as expected """
    test_database()
    test_table()
    test_shape()
    test_columns()

