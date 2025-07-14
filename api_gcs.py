"""Function importing measurement data from open repository and generating it to csv."""
import logging

# Importing modules.
import requests as req
import pandas as pd
import io
from google.cloud import storage
from typing import Union, Dict, List
from google.cloud.exceptions import GoogleCloudError


"""Input data used for data extraction, filtering, fetching and log creation."""
# Dict with location data urls.
locations = {
    "https://api.openaq.org/v3/locations/7961/sensors": ["Bydgoszcz", "Warszawska st."],
    "https://api.openaq.org/v3/locations/7245/sensors": ["Warsaw", "Kondratowicza st."],
    "https://api.openaq.org/v3/locations/10605/sensors": ["Katowice", "Kossutha st."],
    "https://api.openaq.org/v3/locations/9273/sensors": ["Gorzow Wlkp.", "Kosynierow Gdynskich st."],
    "https://api.openaq.org/v3/locations/10566/sensors": ["Wroclaw", "Conrad-Korzeniowski cst."],
}

# API Key.
header = {"X-API-Key": "014cb4740d53cc185d2f13b14e29f6803ecd4cf252e2cc47f1be8bd29bd5d3e6"}

# Choosing the data from report.
selected_columns = ["city", "location", "parameter.name", "latest.value", "latest.datetime.utc"]

# Filtering parameters.
params = ["pm25", "pm10", "no2", "o3"]

# Set up for logging automation/monitoring.
logging.basicConfig(level = logging.INFO)



"""Functions"""

def fetch_data(url: str, header: dict) -> Union[dict, None]:
    """Fetching data from API and collecting JSON.

    :param
        -url(str): API endpoint urls for certain location (Warsaw and London).
        -header(dict): API key.

    :returns
        -dict: JSON data fetch was successful.
        -None: JSON data fetch was not successful.
    """

    # Fetching the data. Checking if API responses and returns correct JSON file.
    try:
        response = req.get(url = url, headers = header)
        response.raise_for_status()
        return response.json()

    except ValueError:
        logging.warning(f"Incorrect JSON file from {url}")
        return None

    except req.exceptions.RequestException as exc:
        logging.warning(f"{url} is not responding: {exc}")



def normalize_data(json_data:dict, city: str, location: str) -> Union[pd.DataFrame, None]:
    """Data processing and manipulation for gathered JSON.

    :param
        -json_data(dict): Fetched JSON data from fetch_data function.
        -city(str): Name of a city where the data was gathered.
        -location(str): Name of a meteorological station in a city.

    :returns
        -pd.DataFrame: DF of a JSON data after normalization (successful).
        -None: Normalization was not successful.
    """

    # Checking if data format is correct ("results" is present and is a list).
    results = json_data.get("results", [])

    if not isinstance(results, list):
        return None

    elif len(results) == 0:
        return None

    # Loading the data to DataFrame - flattening nested structure.
    df = pd.json_normalize(results)

    # Adding "city" and "location" to DataFrame.
    df["city"] = city
    df["location"] = location

    # Converting date.
    df["date"] = pd.to_datetime(df["latest.datetime.utc"]).dt.strftime("%d:%m:%Y")
    df["time"] = pd.to_datetime(df["latest.datetime.utc"]).dt.strftime("%H:%M:%S")

    # Selecting and appending the data.
    col_select = [col for col in selected_columns + ["date", "time"] if col in df.columns]

    # Choosing the the data.
    pick_data = df[col_select]

    # Filtering parameters.
    filter_data = pick_data[pick_data["parameter.name"].isin(params)]

    # Pivoting over parameters.
    final_data = filter_data.pivot(
        index = ["city", "location", "date", "time"],
        columns = "parameter.name",
        values = "latest.value"
    ).reset_index()

    return final_data



def save_to_file(df: pd.DataFrame, bucket_name: str, destination_name: str) -> Union[str, None]:
    """Saving DataFrame as CSV to Google Cloud Storage bucket.

    :param
        -df: DF of a normalized data from normalize_data function.
        -bucket_name: Name of a bucket for saving on GCS.
        -destination_name: Name of a destination for saving on GCS.

    :returns
        str: Path after successful GCS client initiation.
        None: Unsuccessful converting, initiation or file saving on GCS.
    """

    # Converting df to csv.
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index = False)
        csv_buffer.seek(0)

    except (IOError, GoogleCloudError) as err:
        logging.warning(f"Error during converting a file: {err}")
        return None

    #Initiating GCS client.
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_name)
        blob.upload_from_string(csv_buffer.getvalue(), content_type = "text/csv")
        return f"gs://{bucket_name}/{destination_name}"

    except (IOError, GoogleCloudError) as err:
        logging.warning({f"Error during client initiation: {err}"})
        return None



def run(request) -> str:
    """Main function for orchestrating all script.

    :param
        -request: Condition used by Google Cloud Storage.

    :returns
        -str: Information whether data was or was not correctly collected and saved.

    """

    # Creating empty DataFrame for data storing.
    data = pd.DataFrame()

    # Fetching the data and appending to the list.
    for url, (city, location) in locations.items():
        json_data = fetch_data(url, header)

        if json_data is None:
            continue

        df = normalize_data(json_data,city, location)
        if df is not None:
            data.append(df)


    if not data:
        logging.warning("No data collected.")
        return "No data collected."

    elif data.empty:
        logging.warning("Final data is empty - no data to save.")
        return "Final data is empty - no data to save."

    # Setting bucket name. Needs to be updated when implementing into GCS.
    bucket_name = "input_bucket_name"

    destination_name = "results.csv"

    # Saving the file to GCS.
    gcs_dest = save_to_file(df, bucket_name, destination_name)

    if gcs_dest is None:
        return "Failed to upload the file to GCS."

    return f"File uploaded to {gcs_dest}"


def run2():
    data = []

    # Fetching the data and appending to the list.
    for url, (city, location) in locations.items():
        json_data = fetch_data(url, header)

        if json_data is None:
            continue

        df = normalize_data(json_data,city, location)
        if df is not None:
            data.append(df)

    concat_data = pd.concat(data,ignore_index = True)

    if not data:
        logging.warning("No data collected.")
        return "No data collected."

    elif concat_data.empty:
        logging.warning("Final data is empty - no data to save.")
        return "Final data is empty - no data to save."

    return concat_data.to_csv("results.csv", index = False)


run2()

