import pandas as pd
from pandas import DataFrame
import sqlalchemy
from urllib.request import urlretrieve
import zipfile
import unicodedata



def read_zip(url: str, filename: str):
    zip_file, _ = urlretrieve(url=url, filename="temp.zip")
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall("extracted_files")
    file_path = f"extracted_files/{filename}"
    return pd.read_csv(file_path, delimiter=",", encoding='utf-8', encoding_errors="ignore", engine='python', index_col=False)

def extract_relevant_columns(df: DataFrame, relevant_columns: list):
    return df[relevant_columns]

def filter_data(df: DataFrame, filters: dict):
    for key,value in filters.items():
        df = df[df[key] == value]
    return df

def drop_invalid_rows(df: DataFrame):
    # Check for valid stop_lat and stop_lon
    lat_valid = df['stop_lat'].between(-90, 90, inclusive=True)
    lon_valid = df['stop_lon'].between(-90, 90, inclusive=True)

    # Combine all validity checks
    valid_rows = lat_valid & lon_valid

    # Drop invalid rows from the DataFrame
    df_valid = df[valid_rows].copy()

    return df_valid

def get_dtypes(df: DataFrame): 
    meta_data = list(df)

    sql_dtypes = [
        sqlalchemy.INTEGER(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.FLOAT(), 
        sqlalchemy.FLOAT(), 
        sqlalchemy.INTEGER()
    ]

    return dict(zip(meta_data, sql_dtypes))

def save_in_db(df: DataFrame, db_name: str, table_name: str, d_types: dict):
    df.to_sql(table_name, 'sqlite:///' + db_name + '.sqlite', if_exists='replace', index=False, dtype=d_types)


def execute_pipeline():

    url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
    filename = 'stops.txt'
    relevant_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
    filters = {"zone_id": 2001}

    df = read_zip(url=url, filename=filename)
    df = extract_relevant_columns(df=df,relevant_columns=relevant_columns)
    df = filter_data(df=df, filters=filters)
    df = drop_invalid_rows(df=df)

    d_types = get_dtypes(df=df)
    db_name = "gtfs"
    table_name = "stops"
    save_in_db(df=df, db_name=db_name, table_name=table_name, d_types=d_types)

    print(df)


if __name__ == '__main__':
    execute_pipeline()

