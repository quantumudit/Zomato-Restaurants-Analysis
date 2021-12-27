# -- Importing Libraries -- #

print('\n')
print('Importing libraries to perform ETL...')

import numpy as np
import pandas as pd
import geopy
import pyfiglet

import os
from dotenv import load_dotenv
load_dotenv()

import warnings
warnings.filterwarnings('ignore')

print('Initiating ETL Process...')
print('\n')

# -- Starting ETL Process --#

etl_title = "ZOMATO DATA ETL"
ascii_art_title = pyfiglet.figlet_format(etl_title, font='small')
print(ascii_art_title)
print('\n')

# -- Connecting to Dataset -- #

print('Connecting to source datasets')

source_data_1 = pd.read_csv('../01_SOURCE/zomato_raw_data_1.csv', index_col=None)
source_data_2 = pd.read_csv('../01_SOURCE/zomato_raw_data_2.csv', index_col=None)
source_data_3 = pd.read_csv('../01_SOURCE/zomato_raw_data_3.csv', index_col=None)
source_data_4 = pd.read_csv('../01_SOURCE/zomato_raw_data_4.csv', index_col=None)
source_data_5 = pd.read_csv('../01_SOURCE/zomato_raw_data_5.csv', index_col=None)

print('\n')

# -- Combining Individual Dataframes -- #

print('Combining the individual dataframes to create source data')

source_data = source_data_1.append(source_data_2).append(source_data_3).append(source_data_4).append(source_data_5)

print(f'Shape of the source data: {source_data.shape}')
print('\n')

# -- Removing Unnecessary Columns --#

print('Removing unnecessary columns')
print(f'Existing columns in the dataset: {list(source_data)}')
print(f'Existing shape of the dataset: {source_data.shape}')
print('\n')

keep_columns = ['listed_in(city)', 'listed_in(type)', 'online_order', 'book_table', 'approx_cost(for two people)', 'rate',  'votes']
required_data = source_data[keep_columns]

print(f'Columns in the dataset after removing unnecessary columns: {list(required_data)}')
print(f'Shape of the dataset after removing unnecessary columns: {required_data.shape}')
print('\n')

# -- Replacing Unwanted Characters --#

print(f'Unique items in "rate" column:\n{required_data["rate"].unique()}\n')
print(f'Replacing unwanted characters from "rate" column')

replacer_dict = {
    '-': np.nan,
    'NEW': np.nan
}

required_data['rate'].replace(replacer_dict, inplace=True)
required_data['rate'] = required_data['rate'].str.replace(' /5','').str.replace('/5','')

print(f'Unique items in "rate" column after cleaning:\n{required_data["rate"].unique()}\n')
print('\n')

print(f'Unique items in "approx_cost(for two people)" column:\n{required_data["approx_cost(for two people)"].unique()}\n')
print(f'Replacing unwanted characters from "approx_cost(for two people)" column')

required_data['approx_cost(for two people)'] = required_data['approx_cost(for two people)'].str.replace(',','')

print(f'Unique items in "approx_cost(for two people)" column after cleaning:\n{required_data["approx_cost(for two people)"].unique()}\n')
print('\n')

print(f'Unique items in "listed_in(type)" column:\n{required_data["listed_in(type)"].unique()}\n')
print(f'Replacing unwanted characters from "listed_in(type)" column')

required_data['listed_in(type)'] = required_data['listed_in(type)'].str.replace('Drinks & nightlife','Drinks & Night Life')

print(f'Unique items in "listed_in(type)" column after cleaning:\n{required_data["listed_in(type)"].unique()}\n')
print('\n')

# -- Assigning Correct Datatype to Fields --#

print(f'Datatype of existing fields:\n{required_data.dtypes}\n')
print('Changing the datatype of "rate" and "approx_cost(for two people)" columns')

required_data['approx_cost(for two people)'] = required_data['approx_cost(for two people)'].astype('float')
required_data['rate'] = required_data['rate'].astype('float')

print(f'Datatype of fields after fixing datatypes:\n{required_data.dtypes}\n')
print('\n')

# -- Data Grouping --#

print('Summarizing dataset')

group_by_cols = ['listed_in(city)','listed_in(type)','online_order','book_table']

# Grouping data by mean of numerical columns:

grouped_data_A = required_data.groupby(group_by_cols, as_index=False).mean()

# Creating a subset of data to get restaurant count by suburb

subset_B = required_data[group_by_cols]
subset_B['Restaurat Count'] = 1

grouped_data_B = subset_B.groupby(group_by_cols, as_index=False).count()

# merging the two grouped dataframes to create one final grouped dataset

grouped_data = pd.merge(grouped_data_A, grouped_data_B, how='inner', on=group_by_cols)

print(f'Columns in grouped dataset: {list(grouped_data.columns)}')
print(f'Shape of the grouped dataset: {grouped_data.shape}')
print('\n')

# -- Renaming Columns --#

print('Renaming columns')

new_column_names = ['Suburb', 'Restaurant Type', 'Online Order Facility', 'Dine-in Facility', 'Avg Approx Cost (For Two People)','Avg Rating','Avg Votes','Restaurant Count'
]

grouped_data.columns = new_column_names

print(f'Columns in grouped dataset: {list(grouped_data.columns)}')
print('\n')

# -- Geocoding Suburbs -- #

print('Geocoding Suburbs\n')

suburb_df = pd.DataFrame()

suburb_df['Suburb'] = grouped_data['Suburb'].unique()
suburb_df.shape

lat = []
long = []

API_KEY = os.getenv('BING_API_KEY')
geolocator = geopy.geocoders.Bing(api_key=API_KEY, timeout=5)

for suburb in suburb_df['Suburb']:
    
    print(f'Geocoding: {suburb}')
    
    address = f'{suburb}, Bangalore, Karnataka, India'
    
    if suburb is None:
        lat.append(np.nan)
        long.append(np.nan)
    else:
        location = geolocator.geocode(address)
        lat.append(location.latitude)
        long.append(location.longitude)

suburb_df['Latitude'] = lat
suburb_df['Longitude'] = long

print('\n')

# -- Merging "suburb_df" and "grouped_data" --#

print('Merging "suburb_df" and "grouped_data"')

zomato_df = pd.merge(grouped_data, suburb_df, how='left', on='Suburb')

print('\n')

# -- Adding Custom Index Column -- #

print('Adding custom index column to the dataframe')

custom_index_col = pd.RangeIndex(start=1000, stop=1000+len(zomato_df), step=1, name='UniqueID')

zomato_df.index = custom_index_col
zomato_df.index = 'ZM' + zomato_df.index.astype('string')

print(f'Is the index column unique: {zomato_df.index.is_unique}\n')
print(f'Snippet of the transformed dataframe:\n{zomato_df.head()}')
print('\n')

# -- Exporting Data to CSV File --#

print('Exporting the dataframe to CSV file...')

zomato_df.to_csv('../03_DATA/zomato_bengaluru_restaurants_data.csv', encoding='utf-8', index_label='UniqueID')

print('Data exported to CSV...')
print('ETL Process completed !!!')