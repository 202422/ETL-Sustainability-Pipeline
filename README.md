# ETL-Sustainability-Pipeline
This project implements a complete ETL (Extract, Transform, Load) pipeline. The goal is to acquire air quality and sustainability-related data from an open API, clean and transform the data and then load the final dataset into a relational database (SQLite).


## Data Acquisition

I developed a Python script `ExtractTask_ETL.ipynb` in Extract folder to fetch hourly weather data from the Open-Meteo API (https://api.open-meteo.com/v1/forecast) for sustainability analysis, including temperature, humidity, precipitation, wind speed, and weather codes.

### 1. Fetching Data from an API
The script retrieves data for four French cities (Paris, Lyon, Marseille, Bordeaux) using their coordinates. It uses two functions:

- **fetch_weather_data(latitude, longitude, needed_hours)**: Fetches up to 3000 hours of data per location via iterative API calls (90-day limit per call), storing results in a pandas DataFrame with added metadata (latitude, longitude).
- **collect_all_locations(locations, needed_hours)**: Iterates over locations, combines DataFrames, and saves 12,000 records to "weather_data_extracted.csv".

### 2. Error Handling
- **HTTP Errors**: A `requests` session with a `Retry` strategy handles temporary errors (429, 500, 502, 503, 504) with up to 3 retries and exponential backoff (1s, 2s, 4s). For 429 (rate limit), it waits 10 seconds.
- **Specific Errors**: Handles 404 errors (invalid URL/parameters) and logs other HTTP errors.
- **Connection Errors**: Catches network issues (e.g., timeouts) with clear messages.
- **Missing Data**: Checks for missing "hourly" data in API responses and stops if none is returned.
- **API Limits**: Includes pauses (1s between requests, 2s between locations) to avoid rate limits.

### Results
The script collected 12,000 records (7392 non-null for weather variables) and saved them to a CSV file. It meets both requirements by fetching sustainability data via an API and robustly handling errors like rate limits and missing data.

## Data Cleaning

In this Notebook `Cleaning_Task.ipynb` in Transform folder, I cleaned, the extracted dataset.

### 1. Initial Dataset
Pandas DataFrame with 12,000 entries and 8 columns: `time` (object), `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `weather_code` (float64), `latitude`, `longitude`. Columns except `time`, `latitude`, `longitude` had 7,392 non-null entries, with missing values from March 14, 2025, 23:00:00 to January 24, 2025, 00:00:00.

### 2. Cleaning Steps
- **Data Types**: Converted `time` to `datetime64[ns]` and `weather_code` to category.
- **Sorting**: Sorted by `time` (descending), reset index.
- **Missing Values**: Kept first third of rows with missing values (~1,536), dropped others, yielding 8,928 rows.
- **Location Split**: Divided into four DataFrames by unique `latitude` and `longitude`.
- **Imputation**: Used SVR (linear kernel, 72-step sequences) for numerical columns (`temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`). Imputed `weather_code` with mode.
- **Final Dataset**: Concatenated to 8,928 entries, no missing values, no duplicates.
