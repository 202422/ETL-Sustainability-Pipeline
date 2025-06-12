#!/usr/bin/env python
# coding: utf-8


# In[ ]:


import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import List, Tuple, Optional
from sklearn.svm import SVR
from typing import List, Tuple, Dict
import reverse_geocoder as rg
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, Column, Float, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# # Extraction

# In[ ]:


# Global configuration
class WeatherConfig:
    API_URL = "https://api.open-meteo.com/v1/forecast"
    HOURLY_VARIABLES = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "wind_speed_10m",
        "weather_code"
    ]
    MAX_DAYS_PER_REQUEST = 90
    RETRY_CONFIG = {
        "total": 3,
        "backoff_factor": 1,
        "status_forcelist": [429, 500, 502, 503, 504],
        "allowed_methods": ["GET"]
    }


# In[ ]:


# Configure HTTP session with retry mechanism
def setup_session() -> requests.Session:
    """Set up and return a session with retry configuration."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Retry(**WeatherConfig.RETRY_CONFIG))
    session.mount("https://", adapter)
    return session


# In[ ]:


# Construct parameters for API request
def build_api_params(latitude: float, longitude: float, start_date: str, end_date: str) -> dict:
    """Build API request parameters."""
    return {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(WeatherConfig.HOURLY_VARIABLES),
        "timezone": "Europe/Paris"
    }


# In[ ]:


# Fetch a single chunk of weather data for a specific period
def fetch_weather_chunk(
    session: requests.Session,
    latitude: float,
    longitude: float,
    start_date: datetime,
    end_date: datetime
) -> Optional[pd.DataFrame]:
    """Fetch a chunk of weather data for a given period."""
    try:
        params = build_api_params(
            latitude,
            longitude,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        response = session.get(WeatherConfig.API_URL, params=params, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        if "hourly" not in data:
            print(f"No hourly data available for {latitude, longitude}")
            return None
            
        df = pd.DataFrame(data["hourly"])
        df["latitude"] = latitude
        df["longitude"] = longitude
        return df
        
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            print(f"Rate limit reached for {latitude, longitude}. Waiting...")
            time.sleep(10)
            return None
        elif response.status_code == 404:
            print(f"Error 404: Invalid URL or parameters for {latitude, longitude}")
            return None
        else:
            print(f"HTTP error for {latitude, longitude}: {str(http_err)}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Connection error for {latitude, longitude}: {str(e)}")
        return None


# In[ ]:


# Collect all weather data for a single location
def fetch_location_data(
    session: requests.Session,
    latitude: float,
    longitude: float,
    needed_hours: int
) -> pd.DataFrame:
    """Fetch all weather data for a given location."""
    all_data = []
    remaining_hours = needed_hours
    end_date = datetime.now()
    
    while remaining_hours > 0:
        chunk_days = min(WeatherConfig.MAX_DAYS_PER_REQUEST, (remaining_hours // 24) + 1)
        start_date = end_date - timedelta(days=chunk_days)
        
        chunk_df = fetch_weather_chunk(session, latitude, longitude, start_date, end_date)
        
        if chunk_df is not None:
            all_data.append(chunk_df)
            fetched_hours = len(chunk_df)
            remaining_hours -= fetched_hours
            print(f"{fetched_hours} hourly records added for {latitude, longitude} (remaining: {remaining_hours})")
        else:
            break
            
        end_date = start_date - timedelta(days=1)
        time.sleep(1)  # Respect API rate limits
        
        if chunk_df is not None and len(chunk_df) == 0:
            break
    
    final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    return final_df.iloc[:needed_hours]


# In[ ]:


# Orchestrate data collection for multiple locations
def collect_weather_data(
    locations: List[Tuple[float, float]],
    needed_hours: int
) -> pd.DataFrame:
    """Main pipeline to collect weather data for all locations."""
    session = setup_session()
    all_dfs = []
    
    for lat, lon in locations:
        print(f"\n=== Fetching data for location ({lat}, {lon}) ===")
        df_location = fetch_location_data(session, lat, lon, needed_hours)
        
        if not df_location.empty:
            all_dfs.append(df_location)
        
        time.sleep(2)  # Pause between locations
    
    final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    
    # Summary
    if not final_df.empty:
        counts = final_df["latitude"].value_counts()
        print("\n=== Summary ===")
        print(f"Total records retrieved: {len(final_df)}")
        print("Breakdown by location:")
        print(counts.to_string())
    
    return final_df


# In[ ]:


# Save DataFrame to CSV file
def save_to_csv(df: pd.DataFrame, filename: str) -> None:
    """Save the DataFrame to a CSV file."""
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


# In[ ]:


# Entry point for the weather data pipeline
def Extract_main():
    """Main function to run the weather data pipeline."""
    LOCATIONS = [
        (48.8566, 2.3522),  # Paris
        (45.7640, 4.8357),  # Lyon
        (43.2965, 5.3698),  # Marseille
        (44.8378, -0.5792)  # Bordeaux
    ]
    NEEDED_HOURS = 3000
    
    # Run the pipeline
    df_meteo = collect_weather_data(LOCATIONS, NEEDED_HOURS)
    if not df_meteo.empty:
        save_to_csv(df_meteo, "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_data_extracted.csv")


# # Cleaning

# In[ ]:


# Configuration class for cleaning pipeline settings
class CleaningConfig:
    SEQ_SIZE = 72
    COLS_TO_IMPUTE = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]
    SVR_PARAMS = {"kernel": "linear", "gamma": "auto", "C": 1.0, "epsilon": 0.1}


# In[ ]:


# Convert time and weather_code columns to appropriate formats
def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert time to datetime and weather_code to category, then sort by time."""
    df_clean = df.copy()
    df_clean["time"] = pd.to_datetime(df_clean["time"], format="%Y-%m-%dT%H:%M")
    df_clean["weather_code"] = df_clean["weather_code"].astype("category")
    df_clean.sort_values(by="time", ascending=False, inplace=True, ignore_index=True)
    return df_clean


# In[ ]:


# Filter rows to keep only the first third of rows with missing values
def filter_missing_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the first third of rows with missing values and all non-missing rows."""
    nan_rows = df[df.isna().any(axis=1)]
    n = len(nan_rows)
    first_third_nan_rows = nan_rows.iloc[:n // 3]
    return pd.concat([
        df[~df.isna().any(axis=1)],  # Rows without NaN
        first_third_nan_rows         # First third of rows with NaN
    ], ignore_index=True)


# In[ ]:


# Split dataframe into separate dataframes by location
def split_by_location(df: pd.DataFrame) -> List[pd.DataFrame]:
    """Split dataframe into separate dataframes based on unique latitude-longitude pairs."""
    locs = df[["latitude", "longitude"]].drop_duplicates().values
    return [
        df[(df["latitude"] == lat) & (df["longitude"] == lon)].copy()
        for lat, lon in locs
    ]


# In[ ]:


# Convert time series data into sequences for SVR training
def to_sequence(data: pd.Series, seq_size: int = CleaningConfig.SEQ_SIZE) -> Tuple[np.ndarray, np.ndarray]:
    """Convert a time series into sequences for SVR training."""
    x, y = [], []
    data = data.reset_index(drop=True)
    
    for i in range(len(data) - seq_size):
        window = data[i:(i + seq_size)].values
        after_window = data[i + seq_size]
        x.append(window)
        y.append(after_window)
    
    return np.array(x), np.array(y)


# In[ ]:


# Train SVR models for numerical columns
def train_svr_models(df: pd.DataFrame, cols_to_impute: List[str]) -> dict:
    """Train SVR models for each numerical column to impute missing values."""
    models = {}
    for col in cols_to_impute:
        print(f"Processing column {col}")
        series = df[col].dropna()
        X, y = to_sequence(series, seq_size=CleaningConfig.SEQ_SIZE)
        
        model = SVR(**CleaningConfig.SVR_PARAMS)
        print("Training model...")
        model.fit(X, y)
        print("Training completed.")
        
        models[col] = model
    
    return models


# In[ ]:


# Impute missing values in a dataframe using SVR and mode for weather_code
def impute_dataset(df: pd.DataFrame, cols_to_impute: List[str]) -> pd.DataFrame:
    """Impute numerical columns with SVR models and weather_code with mode."""
    df_imputed = df.copy().reset_index(drop=True)
    models = train_svr_models(df_imputed, cols_to_impute)
    
    for col in cols_to_impute:
        print(f"\n=== Imputing column: {col} ===")
        model = models[col]
        nan_indices = df_imputed[df_imputed[col].isna()].index
        
        for idx in nan_indices:
            start_idx = idx - CleaningConfig.SEQ_SIZE
            if start_idx < 0:
                continue
                
            window = df_imputed.loc[start_idx:idx - 1, col]
            if window.isna().any() or len(window) != CleaningConfig.SEQ_SIZE:
                continue
                
            input_seq = window.values.reshape(1, -1)
            prediction = model.predict(input_seq)[0]
            df_imputed.at[idx, col] = prediction
    
    if "weather_code" in df_imputed.columns:
        mode_weather = df_imputed["weather_code"].mode()[0]
        df_imputed["weather_code"] = df_imputed["weather_code"].fillna(mode_weather)
    
    return df_imputed


# In[ ]:


# Process all location dataframes and combine results
def process_locations(dfs: List[pd.DataFrame], cols_to_impute: List[str]) -> pd.DataFrame:
    """Impute missing values for each location dataframe and combine results."""
    imputed_dfs = []
    
    for i, df in enumerate(dfs, start=1):
        print(f"\n=== Processing dataset df_loc{i} ===")
        df_imputed = impute_dataset(df, cols_to_impute)
        imputed_dfs.append(df_imputed)
    
    final_df = pd.concat(imputed_dfs, ignore_index=True)
    final_df.sort_values(by="time", ascending=False, inplace=True, ignore_index=True)
    return final_df


# In[ ]:


# Save the final cleaned dataframe to CSV
def save_cleaned_data(df: pd.DataFrame, filename: str) -> None:
    """Save the cleaned dataframe to a CSV file."""
    df.to_csv(filename, index=False)
    print(f"Cleaned data saved to {filename}")


# In[ ]:


# Main pipeline for cleaning weather data
def clean_weather_data(input_file: str, output_file: str) -> pd.DataFrame:
    """Execute the complete cleaning pipeline for weather data."""
    # Load data
    df = pd.read_csv(input_file)
    
    # Preprocess data
    df_clean = preprocess_dataframe(df)
    
    # Filter missing rows
    df_filtered = filter_missing_rows(df_clean)
    
    # Split by location
    location_dfs = split_by_location(df_filtered)
    
    # Process and impute all locations
    final_df = process_locations(location_dfs, CleaningConfig.COLS_TO_IMPUTE)
    
    # Save results
    save_cleaned_data(final_df, output_file)
    
    return final_df


# In[ ]:


# Entry point for the cleaning pipeline
def Cleaning_main():
    """Run the weather data cleaning pipeline."""
    INPUT_FILE = "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_data_extracted.csv"
    OUTPUT_FILE = "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_data_cleaned.csv"
    
    clean_weather_data(INPUT_FILE, OUTPUT_FILE)


# # Transforming

# In[ ]:


# Configuration class for transformation pipeline settings
class TransformConfig:
    COLUMNS_TO_NORMALIZE = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "heat_index"]
    TIME_BINS = [0, 6, 12, 18, 24]
    TIME_LABELS = ["Night", "Morning", "Afternoon", "Evening"]
    AGGREGATION_DICT = {
        "temperature_2m": "mean",
        "relative_humidity_2m": "mean",
        "precipitation": "mean",
        "wind_speed_10m": "mean",
        "weather_code": lambda x: x.mode()[0],
        "location": "first"
    }


# In[ ]:


# Preprocess dataframe by converting column types
def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert time to datetime and weather_code to category."""
    df_clean = df.copy()
    df_clean["time"] = pd.to_datetime(df_clean["time"], format="%Y-%m-%d %H:%M:%S")
    df_clean["weather_code"] = df_clean["weather_code"].astype("category")
    return df_clean


# In[ ]:


# Perform reverse geocoding and create location dataframe
def add_location_data(df: pd.DataFrame) -> pd.DataFrame:
    """Add location names to the dataframe using reverse geocoding."""
    locs = df[["latitude", "longitude"]].drop_duplicates().values
    coordinates = [(lat, lon) for lat, lon in locs]
    results = rg.search(coordinates)
    
    location_data = [
        {"latitude": lat, "longitude": lon, "location": f"{res['name']}"}
        for (lat, lon), res in zip(locs, results)
    ]
    location_df = pd.DataFrame(location_data)
    
    df_with_locations = df.merge(location_df, on=["latitude", "longitude"], how="left")
    df_with_locations["time"] = pd.to_datetime(df_with_locations["time"], format="%Y-%m-%d %H:%M:%S")
    df_with_locations["weather_code"] = df_with_locations["weather_code"].astype("category")
    df_with_locations["location"] = df_with_locations["location"].astype("category")
    
    return df_with_locations


# In[ ]:


# Aggregate data by time of day and location
def aggregate_by_time_of_day(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data by aggregating based on time of day and location."""
    df_transformed = df.copy()
    df_transformed["time_of_day"] = pd.cut(
        df_transformed["time"].dt.hour,
        bins=TransformConfig.TIME_BINS,
        labels=TransformConfig.TIME_LABELS,
        right=False,
        include_lowest=True
    )
    df_transformed["date"] = df_transformed["time"].dt.date
    
    result = df_transformed.groupby(
        ["latitude", "longitude", "date", "time_of_day"],
        observed=True
    ).agg(TransformConfig.AGGREGATION_DICT).reset_index()
    
    result["time_of_day"] = result["time_of_day"].astype("category")
    result["weather_code"] = result["weather_code"].astype("category")
    result["location"] = result["location"].astype("category")
    
    return result


# In[ ]:


# Calculate heat index for each row
def calculate_heat_index(row: pd.Series) -> float:
    """Calculate heat index based on temperature and relative humidity."""
    T = row["temperature_2m"]
    RH = row["relative_humidity_2m"]
    if T >= 20:
        return T + 0.33 * RH - 0.7
    return T


# In[ ]:


# Add heat index and raining indicator columns
def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add heat index and is_raining columns to the dataframe."""
    df_features = df.copy()
    df_features["heat_index"] = df_features.apply(calculate_heat_index, axis=1)
    df_features["is_raining"] = (df_features["precipitation"] > 0).astype(int)
    return df_features


# In[ ]:


# Normalize specified columns using StandardScaler
def normalize_data(df: pd.DataFrame, columns_to_normalize: List[str]) -> pd.DataFrame:
    """Normalize specified columns using StandardScaler (Z-score)."""
    df_normalized = df.copy()
    scaler = StandardScaler()
    
    missing_cols = [col for col in columns_to_normalize if col not in df_normalized.columns]
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")
    
    print("\nBefore normalization:")
    print(df_normalized[columns_to_normalize].describe())
    
    df_normalized[columns_to_normalize] = scaler.fit_transform(df_normalized[columns_to_normalize])
    
    print("\nAfter normalization:")
    print(df_normalized[columns_to_normalize].describe())
    
    return df_normalized


# In[ ]:


# Save the transformed dataframe to CSV
def save_transformed_data(df: pd.DataFrame, filename: str) -> None:
    """Save the transformed dataframe to a CSV file."""
    df.to_csv(filename, index=False)
    print(f"Transformed data saved to {filename}")


# In[ ]:


# Main pipeline for transforming weather data
def transform_weather_data(input_file: str, output_file: str) -> pd.DataFrame:
    """Execute the complete transformation pipeline for weather data."""
    # Load data
    df = pd.read_csv(input_file)
    
    # Preprocess data
    df_clean = preprocess_dataframe(df)
    
    # Add location data
    df_with_locations = add_location_data(df_clean)
    
    # Aggregate by time of day
    df_time_of_day = aggregate_by_time_of_day(df_with_locations)
    
    # Add derived features
    df_features = add_derived_features(df_time_of_day)
    
    # Normalize data
    df_normalized = normalize_data(df_features, TransformConfig.COLUMNS_TO_NORMALIZE)
    
    # Save results
    save_transformed_data(df_normalized, output_file)
    
    return df_normalized


# In[ ]:


# Entry point for the transformation pipeline
def transform_main():
    """Run the weather data transformation pipeline."""
    INPUT_FILE = "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_data_cleaned.csv"
    OUTPUT_FILE = "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_data_transformed.csv"
    
    transform_weather_data(INPUT_FILE, OUTPUT_FILE)


# In[ ]:


if __name__ == "__main__":
    transform_main()


# # Loading

# In[ ]:


def define_models(Base):
    """Define SQLAlchemy models for Location, Time, and WeatherMeasurement."""
    class Location(Base):
        __tablename__ = 'location'
        id = Column(Integer, primary_key=True)
        latitude = Column(Float)
        longitude = Column(Float)
        location_name = Column(String)
        weather_data = relationship("WeatherMeasurement", back_populates="location")

    class Time(Base):
        __tablename__ = 'time'
        id = Column(Integer, primary_key=True)
        date = Column(String)
        time_of_day = Column(String)
        weather_data = relationship("WeatherMeasurement", back_populates="time")

    class WeatherMeasurement(Base):
        __tablename__ = 'weather_measurement'
        id = Column(Integer, primary_key=True)
        location_id = Column(Integer, ForeignKey('location.id'))
        time_id = Column(Integer, ForeignKey('time.id'))
        temperature_2m = Column(Float)
        relative_humidity_2m = Column(Float)
        precipitation = Column(Float)
        wind_speed_10m = Column(Float)
        weather_code = Column(Float)
        heat_index = Column(Float)
        is_raining = Column(Integer)
        location = relationship("Location", back_populates="weather_data")
        time = relationship("Time", back_populates="weather_data")
    
    return Location, Time, WeatherMeasurement


# In[ ]:


def setup_database(db_name):
    """Set up SQLite database and return engine and session."""
    engine = create_engine(f'sqlite:///{db_name}')
    Base = declarative_base()
    Location, Time, WeatherMeasurement = define_models(Base)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return engine, session, Location, Time, WeatherMeasurement


# In[ ]:


def process_data(df, session, Location, Time, WeatherMeasurement):
    """Process CSV data and insert into database."""
    location_cache = {}
    time_cache = {}

    for _, row in df.iterrows():
        # Location
        loc_key = (row['latitude'], row['longitude'], row['location'])
        if loc_key not in location_cache:
            loc = Location(latitude=row['latitude'], longitude=row['longitude'], 
                         location_name=row['location'])
            session.add(loc)
            session.flush()
            location_cache[loc_key] = loc.id
        location_id = location_cache[loc_key]

        # Time
        time_key = (row['date'], row['time_of_day'])
        if time_key not in time_cache:
            time = Time(date=row['date'], time_of_day=row['time_of_day'])
            session.add(time)
            session.flush()
            time_cache[time_key] = time.id
        time_id = time_cache[time_key]

        # WeatherMeasurement
        weather = WeatherMeasurement(
            location_id=location_id,
            time_id=time_id,
            temperature_2m=row['temperature_2m'],
            relative_humidity_2m=row['relative_humidity_2m'],
            precipitation=row['precipitation'],
            wind_speed_10m=row['wind_speed_10m'],
            weather_code=row['weather_code'],
            heat_index=row['heat_index'],
            is_raining=row['is_raining']
        )
        session.add(weather)


# In[ ]:


def display_results(session, WeatherMeasurement, limit):
    """Retrieve and display weather measurement records."""
    results = session.query(WeatherMeasurement).limit(limit).all()
    for measurement in results:
        print("---")
        print(f"Date: {measurement.time.date} {measurement.time.time_of_day}")
        print(f"Location: {measurement.location.location_name} "
              f"({measurement.location.latitude}, {measurement.location.longitude})")
        print(f"Temperature: {measurement.temperature_2m} °C")
        print(f"Humidity: {measurement.relative_humidity_2m} %")
        print(f"Precipitation: {measurement.precipitation} mm")
        print(f"Wind Speed: {measurement.wind_speed_10m} m/s")
        print(f"Weather Code: {measurement.weather_code}")
        print(f"Heat Index: {measurement.heat_index} °C")
        print(f"Is Raining: {'Yes' if measurement.is_raining else 'No'}")
    return results


# In[ ]:


def manage_weather_database(csv_path, db_name, limit=5):
    """
    Manages weather data operations by calling modular functions.
    
    Args:
        csv_path (str): Path to the input CSV file
        db_name (str): Name of the SQLite database
        limit (int): Number of records to retrieve and display (default: 5)
    
    Returns:
        list: List of retrieved WeatherMeasurement objects
    """
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Setup database
    engine, session, Location, Time, WeatherMeasurement = setup_database(db_name)
    
    # Process data
    process_data(df, session, Location, Time, WeatherMeasurement)
    
    # Commit changes
    session.commit()
    
    # Display results
    results = display_results(session, WeatherMeasurement, limit)
    
    # Close session
    session.close()
    
    return results

def Load_main():
    """Load the weather data into SQLite database"""
    
    csv_path = "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_data_transformed.csv"
    db_name = "C:\\Users\\HP\\Projet_Data_Science\\ETL-Sustainability-Pipeline\\Automated Pipeline\\weather_database.db"
    

    manage_weather_database(csv_path, db_name, limit=5)