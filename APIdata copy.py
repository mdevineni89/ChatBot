#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 16 00:16:26 2023

@author: sunny
"""

import pandas as pd
import sqlite3
import requests

# Set API key and base URL
api_key = '1958346a766d32be90c959cb0ecbbf5a'
base_url = 'https://api.openweathermap.org/data/2.5/forecast'
# api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}
# Set database name and table name
db_name = 'weather.db'
table_name = 'weather_forecast'

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
        return df
    else:
        print(f"Error: API request failed with status code {response.status_code}: {response.text}")
        return None

# Define a function to get weather data for all US cities based on latitude and longitude
def get_all_weather_data():
    # Read city data from CSV file
    city_data = pd.read_csv('/Users/sunny/Desktop/uscities.csv')
    # city_data = city_data.iloc[:1,:]
    # print(city_data)
    # Get weather data for each city and concatenate into a single DataFrame
    dfs = []
    for index, row in city_data.iterrows():
        print(row)
        lat = row['lat']
        lon = row['lng']
        df = get_weather_data(lat, lon)
        if df is not None:
            dfs.append(df)
    all_data = pd.concat(dfs)
    return all_data

# # Connect to SQL database and insert data
conn = sqlite3.connect(db_name)
df = get_all_weather_data()
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()