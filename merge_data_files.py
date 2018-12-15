import math

import numpy as np
import pandas as pd

#Mortality datafile to Rainfall datafile state mappings.
state_mappings = {
    'PAHANG': 'Pahang',
    'PERAK': 'Perak',
    'JOHOR': 'Johor',
    'KEDAH': 'Kedah',
    'N.SEMBILAN': None,
    'WPKL/PUTRAJAYA': 'Federal territory of Malaysia',
    'SELANGOR': 'Selangor',
    'KELANTAN': 'Kelantan',
    'PERLIS': 'Perlis',
    'SARAWAK': 'Sarawak',
    'TERENGGANU': 'Terengganu',
    'PULAU PINANG': 'Penang',
    'MELAKA': 'Malacca',
    'SABAH': 'Sabah',
    'LABUAN': None
}
#Rainfall datafile to Mortality datafile state mappings.
rainfall_state_mappings = dict(
    [(state_mappings[key], key) for key in state_mappings if state_mappings[key] is not None])
#Hotspots datafile to mortality datafile state mappings.
hotspot_state_mappings = {
    'selangor': 'SELANGOR',
    'WPKL': 'WPKL/PUTRAJAYA',
    'Kelantan': 'KELANTAN',
    'N. Sembilan': 'N.SEMBILAN',
    'P. Pinang': 'PULAU PINANG',
    'Sabah': 'SABAH',
    'Perlis': 'PERLIS',
    'Terengganu': 'TERENGGANU',
    'Johor': 'JOHOR',
    'Melaka': 'MELAKA',
    'Perak': 'PERAK',
    'Pahang': 'PAHANG',
    'Selangor': 'SELANGOR',
    'Sarawak': 'SARAWAK',
    'P.Pinang': 'PULAU PINANG'
}

#Read data files.
rainfall = pd.read_csv('rainfall_by_week.csv')
dengue_deaths = pd.read_csv('dengue_death_by_state_week.csv', dtype={
    'No_of_dengue_case': str
})
dengue_cases = pd.read_csv('dengue_kes_by_state_week.csv', dtype={
    'No_of_dengue_case': str
})
hotspots = pd.read_excel('hotspot.xlsx', header=1)
hotspots.columns = ['year', 'week', 'state', 'district', 'locality', 'total_locality_cases', 'outbreak_duration_days']

#Process mortality datafile cases (death/cases)
def process_cases(df, case_col_title):
    deaths_temp = {}
    for (index, row) in df.iterrows():
        year = deaths_temp.setdefault(row['year'], {})
        week = year.setdefault(row['week'], {})
        case_count = row['No_of_dengue_case']
        if not isinstance(case_count, str) and (math.isnan(case_count) or np.isnan(case_count)):
            case_count = None
        week[row['NEGERI']] = case_count

	#Handle wierdness in datafile for special values NO MORTALITY, MISSING for deaths/cases.
    for year in deaths_temp:
        for week in deaths_temp[year]:
            data = deaths_temp[year][week]
            overview = data.pop('MALAYSIA')
            if overview == 'NO MORTALITY':
                count = 0
                for state in data.keys():
                    if data[state] is None:
                        data[state] = 0
                    elif data[state].isdigit():
                        count += int(data[state])
                    else:
                        count += data[state]
                if count > 0:
                    print(f'NO MORTALITY with total deaths of {count}')
            elif overview == 'MISSING' or None:
                count = 0
                for state in data.keys():
                    if isinstance(data[state], str) and data[state].isdigit():
                        count += int(data[state])
                    elif type(data[state]) == type(count):
                        count += data[state]
                if count > 0:
                    print(f'MISSING with total deaths of {count}')
            elif isinstance(overview, str) and overview.isdigit():
                total = int(overview)
                count = 0
                for state in data.keys():
                    if data[state] is None:
                        data[state] = 0
                    elif isinstance(data[state], str) and data[state].isdigit():
                        count += int(data[state])
                    elif type(data[state]) == type(count):
                        count += data[state]
                if count != total:
                    print(f'Mismatched counts: {count}/{total}')

    columns = ('state', 'year', 'week', case_col_title)

    tuples = [(state, year, week, deaths_temp[year][week][state])
              for year in deaths_temp
              for week in deaths_temp[year]
              for state in deaths_temp[year][week]]

    return pd.DataFrame(tuples, columns=columns)

#Normalize strings by returning the first occurance of a string for all strings that matches its lowercase, spaces removed form.
def normalize_string(string, dictionary):
    key = string.lower().replace(' ','')
    return dictionary.setdefault(key, string)

#Process data 
dengue_deaths = process_cases(dengue_deaths, 'total_state_deaths')
dengue_cases = process_cases(dengue_cases, 'total_state_cases')
#Normalize state values based on predefined mappings.
rainfall['state'] = [rainfall_state_mappings[state.strip()] for state in rainfall['state']]
hotspots['state'] = [hotspot_state_mappings[state.strip()] for state in hotspots['state']]
#Normalize district and locality values using normalize_string function.
disctrict_dict = dict()
locality_dict = dict()
hotspots['district'] = [normalize_string(district.strip().replace('\n', ';'), disctrict_dict) for district in hotspots['district']]
hotspots['locality'] = [normalize_string(locality.strip().replace('\n', ';'), locality_dict) for locality in hotspots['locality']]

#Merge all datasets and output CSV file.
merged = hotspots.merge(dengue_cases, left_on=('state', 'year', 'week'), right_on=('state', 'year', 'week'))
merged = merged.merge(dengue_deaths, left_on=('state', 'year', 'week'), right_on=('state', 'year', 'week'))
merged = merged.merge(rainfall, left_on=('state', 'year', 'week'), right_on=('state', 'year', 'week'))
merged['year'] = merged['iso_year']
merged.drop(['iso_year'], axis=1)
merged['total_locality_cases'] = [None if isinstance(cases, str) and not cases.isdigit() else int(cases) for cases in merged['total_locality_cases']]
merged.to_csv('merged_data.csv', encoding='utf-8', index=False)

from isoweek import Week

#Merge weekly dataset to monthly using isoweek library.
merged['month_monday'] = merged.apply(
    lambda row: Week(int(row['year']), int(row['week'])).monday().month, axis=1)
merged['total_state_deaths'] = [int(cases) if cases is not None else -1 for cases in merged['total_state_deaths']]
merged['total_state_cases'] = [int(cases) if cases is not None else -1 for cases in merged['total_state_cases']]
merged['total_locality_cases'] = [cases if cases is not None else 0 for cases in merged['total_locality_cases']]
merged_bymonth = merged.groupby(['year', 'month_monday', 'state', 'district', 'locality'], as_index=False) \
    .agg({
    'total_locality_cases': 'sum',
    'outbreak_duration_days': 'mean',
    'total_state_cases': 'sum',
    'total_state_deaths': 'sum',
    'total_rainfall_mm': 'sum',
    'mean_wind_speed': 'mean',
    'total_solar_radiation_mjm2': 'sum',
    'days_in_week': ['sum', 'count'],
})
merged_bymonth.columns = [
    'year', 'month_monday', 'state', 'district', 'locality', 'total_locality_cases', 'outbreak_duration_days',
    'total_state_cases', 'total_state_deaths', 'total_rainfall_mm', 'mean_wind_speed', 'total_solar_radiation_mjm2',
    'days_in_month', 'weeks_in_month'
]
merged_bymonth['total_state_deaths'] = [None if int(cases) < 0 else cases for cases in merged_bymonth['total_state_deaths']]
merged_bymonth['total_state_cases'] = [None if int(cases) < 0 else cases for cases in merged_bymonth['total_state_cases']]
merged_bymonth.to_csv('merged_data_month.csv', encoding='utf-8', index=False)