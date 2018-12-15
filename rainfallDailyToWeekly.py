import datetime
import math

import pandas as pd
import numpy as np

#Read the rainfall by state CSV file.
df = pd.read_csv('./rainfall_by_state.csv')

#Find the list of station numbers
stnnos = [int(x) for x in df["Stnno"] if not math.isnan(x)]
newlist = []

#Deduplicate station number list while preserving order.
for stnno in stnnos:
    if stnno not in newlist:
        newlist.append(stnno)
stnnos = newlist
del newlist

#Initialize temporary variable.
split_by_stnno = {}
for stnno in stnnos:
    split_by_stnno[stnno] = []

#Split dataset by station number.
for row in df.iterrows():
    stnno = row[1]["Stnno"]
    if math.isnan(stnno):
        continue
    stnno = int(stnno)
    split_by_stnno[stnno].append(row[1])

#Calculate week-of-year for each observation.
for stnno in stnnos:
    df1 = pd.DataFrame(split_by_stnno[stnno])
    df1['Week'] = df1.apply(
        lambda row: datetime.date(int(row['Year']), int(row['Month']), int(row['Day'])).isocalendar()[1], axis=1)
    df1['ISOYear'] = df1.apply(
        lambda row: datetime.date(int(row['Year']), int(row['Month']), int(row['Day'])).isocalendar()[0], axis=1)
    split_by_stnno[stnno] = df1

#Merge split dataframes into one.
dfWithWeek = pd.concat([split_by_stnno[stnno] for stnno in stnnos], axis=0, ignore_index=True)
# dfWithWeek['Stnno'] = dfWithWeek['Stnno'].astype(int)
dfWithWeek['Year'] = dfWithWeek['Year'].astype(int)

#Group observations for multiple stations in the same state for the same day.
dfWithWeek = dfWithWeek.groupby(['Year','Month','Day','State'], as_index=False) \
    .agg({
        "Rainfall( 08-08 MST )( mm )": "mean",
        "24 Hour Mean Wind( m/s )": "mean",
        "Solar Radiation( MJm-2)": "mean",
        "Week": "first",
        "ISOYear": "first"
    })

#Drop some columns and assign better column names and create a placeholder variable for counting.
dfWithWeek = dfWithWeek.drop(['Month', 'Day'], axis=1)
dfWithWeek.columns = ['year', 'state', 'rainfall_8to8_mm', '24h_mean_wind', 'solar_radiation_mjm2', 'week', 'iso_year']
dfWithWeek['dummy'] = 1

#Aggregate to weekly level.
aggResult = dfWithWeek.groupby(['state', 'iso_year', 'week'], as_index=False) \
    .agg({
        "year": lambda x:x.value_counts().index[0],
        "rainfall_8to8_mm": "sum",
        "24h_mean_wind": "mean",
        "solar_radiation_mjm2": "sum",
        "dummy": "count"
        # 'station_name': "first",
        # 'station_latitude': "first",
        # 'station_longitude': "first",
        # 'station_elevation': "first",
    })

#Assign column names and set solar radiation to nan if 0, because 0 solar radiation is impossible.
aggResult.columns = ['state', 'iso_year', 'week', 'year', 'total_rainfall_mm', 'mean_wind_speed', 'total_solar_radiation_mjm2', 'days_in_week']
aggResult['total_solar_radiation_mjm2'] = aggResult['total_solar_radiation_mjm2'].map(lambda x: np.nan if x == 0 else x )

#Output to CSV.
aggResult.to_csv('rainfall_by_week.csv', index=False)

