import pandas as pd 
import dateparser
import sqlalchemy
import os
from meteostat import Point, Daily
from datetime import datetime

""" Data-pipeline for dataset 1 """


""" Load Data from source """

df1 = pd.read_csv( \
    'https://offenedaten-konstanz.de/sites/default/files/Zaehlstelle_Herose_2021_15min.csv', \
    sep = ','
)


""" Transform Data so it fits """

df1 = df1.dropna(axis=1, how='all') # drop all columns that only consits of not existing values 

df1.columns.values[0] = 'Date' # column of date
df1.columns.values[1] = 'Total bikers' # column of total amount of bikers driving on the bridge
df1.columns.values[2] = 'Bikers inward' # column of bikers that are driving on the bridge towards the city
df1.columns.values[3] = 'Bikers outward' # column of bikers that are driving on the bridge and are leaving the city

# use a dateparser to change the time column date time to datetime and then normalize it so only the date remains 
df1['Date'] = df1['Date'].apply(lambda x: dateparser.parse(x))
df1['Date'] = df1['Date'].dt.normalize()

# group all data of the the day  and aggregate the values of the other columns
agg_functions = {'Total bikers': 'sum', 'Bikers inward': 'sum', 'Bikers outward': 'sum'}
df1 = df1.groupby(df1['Date'], as_index=False).aggregate(agg_functions)


"""Data loading into a sqllite database"""

df1.to_sql('dataset1', 'sqlite:///data/datasets.sqlite', if_exists='replace', index=False)




""" Data-pipeline for dataset 2 """


""" Load Data from source """

"""Just for testing"""
#script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
#rel_path = "dataset2.csv"
#abs_file_path = os.path.join(script_dir, rel_path)
#df2 = pd.read_csv(abs_file_path, sep = ',') # link to directly download the dataset is not available

# Use the metestat library to get data
start = datetime(2021, 1, 1)
end = datetime(2021, 12, 31)

# Create Point for city of constance
constance = Point(47.6833, 9.1833)

# Get daily data for 2021
data = Daily(constance, start, end)
data = data.fetch()

# put the data in a dataframe and reset the index
df2 = pd.DataFrame(data)
df2 = df2.reset_index()

""" Transform Data so its fits """
df2.columns.values[0] = 'Date' # column of date
df2.columns.values[1] = 'Average temperature' # column of average temperature
df2.columns.values[2] = 'Min. temperature' # column of minimal temperature
df2.columns.values[3] = 'Max. temperature' # column of maximum temperature
df2.columns.values[4] = 'Total rainfall' # column of total amount of rainfall
df2.columns.values[5] = 'Snow-level' # column of snow level
df2.columns.values[6] = 'Wind-direction' # column of wind direction
df2.columns.values[7] = 'Wind-speed' # column of wind speed
df2.columns.values[8] = 'Lace boe' # column of lace boe
df2.columns.values[9] = 'Air pressure' # column of air pressure
df2.columns.values[10] = 'Duration of sunshine' # column of duration of sunshine

# set date to datatype datetime 
df2['Date'] = pd.to_datetime(df2['Date'])


"""Data loading into a sqllite database"""

df2.to_sql('dataset2', 'sqlite:///data/datasets.sqlite', if_exists='replace', index=False)