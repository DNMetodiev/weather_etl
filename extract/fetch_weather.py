import requests
import os
import yaml
import pandas as pd
from sqlalchemy import create_engine

def fetch_weather_data(city):
    """
    Fetches weather data for a specific city using the OpenWeatherMap API.
    Args:
        city (str): The name of the city to fetch weather data for.
    Returns:
        dict: Raw JSON data from the OpenWeatherMap API.
    """
    with open('extract/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    api_key = config['api_key']
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}, {response.text}")
        return None

def process_weather_data(raw_data):
    """
    Transforms raw weather data into a clean, tabular format.
    Args:
        raw_data (dict): Raw JSON response from the weather API.
    Returns:
        pandas.DataFrame: Processed data as a DataFrame.
    """
    processed_data = {
        "city": raw_data.get("name"),
        "temperature": raw_data["main"]["temp"],
        "humidity": raw_data["main"]["humidity"],
        "weather": raw_data["weather"][0]["description"],
        "wind_speed": raw_data["wind"]["speed"],
        "datetime": pd.Timestamp.now()
    }
    return pd.DataFrame([processed_data])

def save_to_csv(dataframe, filename="weather_data.csv"):
    """
    Saves the processed data to a CSV file without adding duplicates.
    Args:
        dataframe (pandas.DataFrame): The processed weather data.
        filename (str): Name of the CSV file.
    """
    if os.path.exists(filename):
        # Load existing data
        existing_data = pd.read_csv(filename)

        # Concatenate new and existing data, then drop duplicates
        combined_data = pd.concat([existing_data, dataframe], ignore_index=True)
        combined_data.drop_duplicates(subset=["city", "datetime"], keep="last", inplace=True)

        # Save the updated data back to the CSV
        combined_data.to_csv(filename, index=False)
    else:
        # If file doesn't exist, save the new data directly
        dataframe.to_csv(filename, index=False)

    print(f"Data saved to {filename}, avoiding duplicates.")

def load_to_database(dataframe, db_name="weather_data.db", table_name="weather"):
    """
    Loads the processed data into a SQLite database.
    Args:
        dataframe (pandas.DataFrame): The processed weather data.
        db_name (str): Name of the SQLite database file.
        table_name (str): Name of the table to store the data.
    """
    engine = create_engine(f"sqlite:///{db_name}")
    dataframe.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"Data loaded into database: {db_name}, table: {table_name}")

if __name__ == "__main__":
    # List of cities to fetch weather data for
    cities = ["London", "Paris", "New York", "Tokyo", "Mumbai"]

    # DataFrame to hold all weather data
    all_data = pd.DataFrame()

    # Fetch and process data for each city
    for city in cities:
        print(f"Fetching weather data for: {city}")
        raw_weather_data = fetch_weather_data(city)
        if raw_weather_data:
            processed_weather_data = process_weather_data(raw_weather_data)
            all_data = pd.concat([all_data, processed_weather_data], ignore_index=True)

    # Display the final DataFrame
    print(all_data)

    # Save all data to a CSV file
    save_to_csv(all_data)

    # Load all data into a SQLite database
    load_to_database(all_data)
