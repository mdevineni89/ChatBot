#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 00:16:26 2023

@author: sunny
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 00:16:26 2023

@author: sunny
"""

import pandas as pd
#import mysql.connector
import requests
import pymysql
from sqlalchemy import create_engine



# Set API key and base URL
api_key = '5d753727bae7375ef4e2717d36efc0ee'
base_url = 'https://api.openweathermap.org/data/2.5/forecast'
# api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}
# Set database name and table name
db_name = 'weather'
table_name = 'weather_forecast'

# Define the activities and their temperature ranges
activities = {
    'Soccer': {'min_temp': 4.4, 'max_temp': 32.2,},
    'Football': {'min_temp': 4.4, 'max_temp': 26.7,},
    'Baseball': {'min_temp': 10, 'max_temp': 32.2},
    'Tennis': {'min_temp': 10, 'max_temp': 35},
    'Golf': {'min_temp': 4.4, 'max_temp': 32.2},
    'Hiking': {'min_temp': 10, 'max_temp': 29.4},
    'Cycling': {'min_temp': 10, 'max_temp': 29.4},
    'Running': {'min_temp': 4.4, 'max_temp': 29.4},
    'Swimming': {'min_temp': 21.1, 'max_temp': 29.4}
}


# Define a function to get weather data for a given latitude and longitude
def get_weather_data(lat, lon):
    # Build API request URL
    url = f'{base_url}?lat={lat}&lon={lon}&units=metric&appid={api_key}'
    
    # Send API request and get JSON response
    response = requests.get(url)
    # print(response.json())
    if response.status_code == 200:
        json_data = response.json()
       
        
        # Extract relevant data from JSON response
        weather_data = []
        for item in json_data['list']:
            date = pd.to_datetime(item['dt'], unit='s')  
            temperature = item['main']['temp']
            humidity = item['main']['humidity']
            pressure = item['main']['pressure']
            wind_speed = item['wind']['speed']
            weather_desc = item['weather'][0]['description']
            weather_data.append([date, temperature, humidity, pressure, wind_speed, weather_desc])
        
        # Create a Pandas DataFrame from the weather data
        df = pd.DataFrame(weather_data, columns=['date', 'temperature', 'humidity', 'pressure', 'wind_speed', 'weather_desc'])
        df['latitude'] = lat
        df['longitude'] = lon
        
        # Add columns for each activity and set their values based on the temperature range
        for activity in activities:
            min_temp = activities[activity]['min_temp']
            max_temp = activities[activity]['max_temp']
            

            # Check if temperature is within the range and weather is favorable
            df[f'{activity}'] = (df['temperature'] >= min_temp) & (df['temperature'] <= max_temp) & ~(df['weather_desc'].isin(['light rain', 'moderate rain', 'heavy intensity rain']))
            df[f'{activity}'] = df[f'{activity}'].replace({True: 'Yes', False: 'No'})
        return df
    
        
        
    else:
        print(f"Error: API request failed with status code {response.status_code}: {response.text}")
        return None

# Define a function to get weather data for all US cities based on latitude and longitude
def get_all_weather_data():
    # Read city data from CSV file
    #city_data = pd.read_csv('/Users/sunny/Desktop/uscities.csv')
    city_data = [
        {'name': 'New York', 'lat': 40.7128, 'lon': -74.0060},
        {'name': 'Los Angeles', 'lat': 34.0522, 'lon': -118.2437},
        {'name': 'Chicago', 'lat': 41.8781, 'lon': -87.6298},
        {'name': 'Houston', 'lat': 29.7604, 'lon': -95.3698},
        {'name': 'Phoenix', 'lat': 33.4484, 'lon': -112.0740}
    ]
    #city_data = city_data.iloc[:5,:]
    # print(city_data)
    # Get weather data for each city and concatenate into a single DataFrame
    # Get weather data for each city and concatenate into a single DataFrame
    dfs = []
    for city in city_data:
        name = city['name']
        lat = city['lat']
        lon = city['lon']
        df = get_weather_data(lat, lon)
        if df is not None:
            df['city'] = name
            dfs.append(df)
    all_data = pd.concat(dfs)
    return all_data

    #dfs = []
    #for index, row in city_data.iterrows()
        #print(row)
        #lat = row['lat']
        #lon = row['lng']
        #df = get_weather_data(lat, lon)
        #if df is not None:
            #dfs.append(df)
    #all_data = pd.concat(dfs)
    #return all_data

# Connect to MySQL database and insert data
# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="SunSri98$$$",
                               db=db_name))


#conn = mysql.connector.connect(**config)
df = get_all_weather_data()
# Write results back to MySQL database
#print(len(df))
df.to_sql(table_name, con = engine, if_exists = 'append', chunksize = 1000)
#df.to_sql(table_name, conn, if_exists='replace', index=False)
#conn.close()
