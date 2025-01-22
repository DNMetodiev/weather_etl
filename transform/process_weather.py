import pandas as pd

def process_weather_data(raw_data):
    processed_data = {
        "city": raw_data.get("name"),
        "temperature": raw_data["main"]["temp"],
        "humidity": raw_data["main"]["humidity"],
        "weather": raw_data["weather"][0]["description"],
        "datetime": pd.Timestamp.now()
    }
    return pd.DataFrame([processed_data])
