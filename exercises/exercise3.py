import pandas as pd
from pandas import DataFrame
import sqlalchemy


def read_csv(source: str):
    usecolumns=[0, 1, 2, 12, 22, 32, 42, 52, 62 , 72]
    return pd.read_csv(source, sep = ';', on_bad_lines='skip', encoding='latin1', skiprows=6, skipfooter=4, usecols=usecolumns, engine='python', index_col=False)

def rename_columns(df: DataFrame):
    df.columns.values[0] = 'date' 
    df.columns.values[1] = 'CIN' 
    df.columns.values[2] = 'name' 
    df.columns.values[3] = 'petrol'
    df.columns.values[4] = 'diesel' 
    df.columns.values[5] = 'gas' 
    df.columns.values[6] = 'electro' 
    df.columns.values[7] = 'hybrid' 
    df.columns.values[8] = 'plugInHybrid' 
    df.columns.values[9] = 'others' 
    return df

def drop_invalid_rows(df: DataFrame):
    df['CIN'] = df['CIN'].map(str)
    df.loc[df["CIN"].str.len() == 4, "CIN"] = str(0) + df["CIN"]
    df = df[df['CIN'].str.len()==5]
    for cols in df.columns.tolist()[3:]:
        df = df[df[cols].replace("-", "-1").map(int)> 0]
    return df

def save_in_db(df: DataFrame, db_name: str, table_name: str, d_types: dict):
    df.to_sql(table_name, 'sqlite:///' + db_name, if_exists='replace', index=False, dtype=d_types)


def get_dtypes(df: DataFrame): 
    meta_data = list(df)

    sql_dtypes = [
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.TEXT(), 
        sqlalchemy.INTEGER(), 
        sqlalchemy.INTEGER(), 
        sqlalchemy.INTEGER(), 
        sqlalchemy.INTEGER(), 
        sqlalchemy.INTEGER(), 
        sqlalchemy.INTEGER(), 
        sqlalchemy.INTEGER()
    ]

    return dict(zip(meta_data, sql_dtypes))


def execute_pipeline(source: str, name: str):
    df = read_csv(source)
    df = rename_columns(df)
    df = drop_invalid_rows(df)
    db_name = name + '.sqlite'
    table_name = name
    d_types = get_dtypes(df)
    print(df)
    save_in_db(df, db_name, table_name, d_types)


if __name__ == '__main__':
    source = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'
    name = 'cars'
    execute_pipeline(source=source, name=name)

