# Project Plan

## Summary

<!-- Describe your data science project in max. 5 sentences. -->
This projects analyzes the amount of daily bicyle drivers driving through a central point of the german city Konstanz. This will then be compared with the weather situation of the corresponding days in Konstanz. Then a correlation between weather aspects like wind-strength, rain or temperature and the amount of bicyles can be created. The goal is to determine wether or wether not weather conditions like temperature, rain and wind have affects on people using bicyles in cities.

## Rationale

<!-- Outline the impact of the analysis, e.g. which pains it solves. -->
The analysis helps bike-sharing providers to better understand the correaltion between weather and the amount of bicycles driving by the example of the german city Konstanz. Therefore, hopefully, a better management of providing bicyles can be achieved.  

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Fahrrad-Dauerzählstellen Konstanz (Mobilithek)
* Metadata URL: https://mobilithek.info/offers/-7161835583190029268
* Data URL: https://offenedaten-konstanz.de/sites/default/files/Zaehlstelle_Herose_2021_15min.csv
* Data Type: CSV

This dataset contains information of the amount of bicycles driving through a central point (Herose) in the german city Konstanz. The datasets contains data for every 15min of every day of the year 2021. It is mentioned how many bicycles are driving in and outside the city for in one hour. 

### Datasource2: Wetter Konstanz (Meteostat)
* Metadata URL: https://meteostat.net/de/station/10929?t=2021-01-01/2021-12-31
* Data URL: https://meteostat.net/de/station/10929?t=2021-01-01/2021-12-31
* Data Type: CSV

This dataset contains information of the daily weather information of the german city Konstanz of 2021, measured by a central weather station. It contains information about minimal, maximal and average temperatures for every day, as well as infotmation about the amount of rain/snow and information about the wind. 

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Building an automated data pipeline [#1] [i1]
2. Write data engineering script for pulling data [#2] [i2]
3. Write data engineering script for massage data [#3] [i3]
4. Write data engineering script for storing data [#4] [i4]
5. Create automated test for project [#5] [i5]
6. Deploy the project on GitHub pages [#6] [i6]

[i1]: https://github.com/leondaniel22/2023-amse-template/issues/1
[i2]: https://github.com/leondaniel22/2023-amse-template/issues/2
[i3]: https://github.com/leondaniel22/2023-amse-template/issues/3
[i4]: https://github.com/leondaniel22/2023-amse-template/issues/4
[i5]: https://github.com/leondaniel22/2023-amse-template/issues/5
[i6]: https://github.com/leondaniel22/2023-amse-template/issues/6