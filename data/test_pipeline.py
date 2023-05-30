import pytest
import os
import pandas as pd
import sqlalchemy
import pipeline_script


def test_data_load():
    """ Test if the data loading works and both datasets are an object of class pandas.DataFrame """
    assert isinstance(pipeline_script.df1, pd.DataFrame)
    assert isinstance(pipeline_script.df2, pd.DataFrame)

def test_dataframe_shape():
    """ Test if the shapes of both dataframes are correct as expected"""
    df1_expected_shape = (365, 4) # expected shape of the dataframe 1
    df2_expected_shape = (365, 11) # expected shape of the dataframe 2
    df1_actual_shape = pipeline_script.df1.shape # actual shape of the dataframe 1
    df2_actual_shape = pipeline_script.df2.shape # actual shape of the dataframe 2
    # check if the shape is correct
    assert len(df1_actual_shape) == 2
    assert len(df2_actual_shape) == 2 
    assert df1_expected_shape[0] == df1_actual_shape[0] 
    assert df1_expected_shape[1] == df1_actual_shape[1]
    assert df2_expected_shape[0] == df2_expected_shape[0]
    assert df2_expected_shape[1] == df2_actual_shape[1]

def test_dataframe_columns():
    """ Test if the columns of both dataframes are correct as expected """
    df1_expected_columns = ['Date', 'Total bikers', 'Bikers inward', 'Bikers outward'] # expected columns of dataframe 1
    df2_expected_columns = ['Date', 'Average temperature', 'Min. temperature', 'Max. temperature',
       'Total rainfall', 'Snow-level', 'Wind-direction', 'Wind-speed',
       'Lace boe', 'Air pressure', 'Duration of sunshine'] # expected columns of dataframe 2
    df1_actual_columns = pipeline_script.df1.columns # actual columns of dataframe 1
    df2_actual_columns = pipeline_script.df2.columns # actual columns of dataframe 2
    # check if the columns are correct
    assert len(df1_actual_columns) == len(df1_expected_columns)
    assert all([a == b for a, b in zip(df1_actual_columns, df1_expected_columns)])
    assert len(df2_actual_columns) == len(df2_expected_columns)
    assert all([a == b for a, b in zip(df2_actual_columns, df2_expected_columns)])

def test_output_exists():
    """ Test if after the execution of the pipeline, both datasets are safed in an sqlite database file in the data directory """
    directory_path = os.path.join(os.getcwd(), 'data/') # get directory path
    assert os.path.exists(os.path.join(directory_path,"dataset1.sqlite"))
    assert os.path.exists(os.path.join(directory_path,"dataset2.sqlite"))

def test_pipeline():
    """ Test if the pipeline script works as expected """
    test_output_exists()
    test_data_load()
    test_dataframe_shape()
    test_dataframe_columns()

if __name__ == "__main__":
    print("Start testing pipeline ...")
    test_pipeline()
    print("Test done!")

