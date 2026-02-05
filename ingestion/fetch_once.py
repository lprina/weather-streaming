"""
Fetch a single OpenWeatherMap One Call API response and store it locally.

This script is intended to be run once during development to avoid exceeding
the OpenWeatherMap daily API rate limit. The stored JSON file can later be
replayed by the ingestion service to simulate a real-time data stream.
"""

import json
import requests

API_KEY = "87600d4493f574b1d19f7cf6c247a6eb"

LAT = 52.084516
LON = 5.115539

URL = (
    "https://api.openweathermap.org/data/3.0/onecall"
    f"?lat={LAT}&lon={LON}"
    "&exclude=hourly,daily,current"
    "&units=metric"
    f"&appid={API_KEY}"
)

def fetch_weather_data() -> dict:
    """
    Fetch weather data from the OpenWeatherMap One Call API.

    Returns
    -------
    dict
        Parsed JSON response from the API containing minutely precipitation
        forecasts and associated metadata.

    Raises
    ------
    requests.HTTPError
        If the API request fails or returns a non-200 response.
    """
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    return response.json()

def save_weather_data(data: dict, path: str) -> None:
    """
    Save weather data to a local JSON file.

    Parameters
    ----------
    data : dict
        Weather data returned by the OpenWeatherMap API.
    path : str
        File path where the JSON data will be stored.
    """
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def main() -> None:
    """
    Fetch weather data once and persist it locally for later replay.
    """
    data = fetch_weather_data()
    save_weather_data(data, "data/weather.json")
    print("Weather data saved to ingestion/data/weather.json")

if __name__ == "__main__":
    main()
