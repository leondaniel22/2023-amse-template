import pandas as pd
from pandas import DataFrame
import sqlalchemy
import numpy as np


def read_csv(source: str):
    return pd.read_csv(source, sep = ';', on_bad_lines='skip')


def save_in_db(df: DataFrame, db_name: str, table_name: str, d_types: dict):
    df.to_sql(table_name, 'sqlite:///' + db_name, if_exists='replace', index=False, dtype=d_types)


def get_dtypes(df: DataFrame): 
    meta_data = list(df)

    sql_dtypes = [
        sqlalchemy.INTEGER(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.REAL(), 
        sqlalchemy.REAL() ,
        sqlalchemy.INTEGER(), 
        sqlalchemy.REAL(), 
        sqlalchemy.CHAR(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.REAL()
    ]

    return dict(zip(meta_data, sql_dtypes))


def execute_pipeline(source: str, name: str):
    df = read_csv(source)
    db_name = name + '.sqlite'
    table_name = name
    d_types = get_dtypes(df)
    save_in_db(df, db_name, table_name, d_types)


if __name__ == '__main__':
    source = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'
    name = 'airports'
    execute_pipeline(source=source, name=name)

