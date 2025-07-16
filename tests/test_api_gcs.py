"""Testing api_gcs script by using pytest"""

# Importing modules.
import pytest
import os
import pandas as pd
import requests.exceptions
import pandas as pd
import importlib

import openaq_data_pipeline.api_gcs
import openaq_data_pipeline.api_gcs as api_gcs
from openaq_data_pipeline.api_gcs import fetch_data, normalize_data, save_to_file, run
from google.cloud.exceptions import GoogleCloudError
from unittest.mock import patch, MagicMock
from unittest.mock import patch, MagicMock



# === Testing fetch_data ===


# Correct API response.
@patch("requests.get")
def test_fetch_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"results": [{"data" : "test"}]}
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None

    mock_get.return_value = mock_response

    url = "https://openaqurl"
    header = {}
    data = fetch_data(url, header)

    assert data == {"results": [{"data" : "test"}]}

    mock_get.assert_called_once_with(url = url, headers = header)


# Parametrization for HTTP error.
@pytest.mark.parametrize("http_error",
                         [requests.exceptions.HTTPError("404 Client Error"),
                         requests.exceptions.HTTPError("400 Bad Request"),
                         requests.exceptions.HTTPError("500 Internal Server Error"),
])
# Incorrect API response (HTTP error).
@patch("requests.get")
def test_fetch_data_http_error(mock_get, http_error):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = http_error

    mock_get.return_value = mock_response

    data = fetch_data("https://openaqurl", header = {})
    assert data is None


# Incorrect JSON.
@patch("requests.get")
def test_fetch_data_incorrect_json(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.side_effect = ValueError("Invalid JSON")

    mock_get.return_value = mock_response

    data = fetch_data("https://openaqurl", header = {})
    assert data is None


# Connection error.
@patch("requests.get")
def test_fetch_data_no_connection(mock_get):
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection failure")

    data = fetch_data("https://openaqurl", header={})
    assert data is None


# Timeout error.
@patch("requests.get")
def test_fetch_data_timeout(mock_get):
    mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

    data = fetch_data("https://openaqurl", header={})
    assert data is None


# Empty API response.
@patch("requests.get")
@patch("logging.warning")
def test_fetch_data_empty(mock_log_warning, mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b""

    mock_get.return_value = mock_response

    url = "https://openaqurl"
    header = {}
    data = fetch_data(url, header)

    assert data is None
    mock_log_warning.assert_called_once_with(f"Empty response from {url}")



# === Testing normalize_data ===

# Correct DataFrame.
def test_normalize_data_success():
    json_data = {
        "results" : [
            {
                "parameter.name" : "pm25",
                "latest.value" : 12,
                "latest.datetime.utc" : "2025-07-15T12:00:00Z"
            },
            {
                "parameter.name": "pm10",
                "latest.value": 20,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            },
            {
                "parameter.name": "o3",
                "latest.value": 17,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            },
            {
                "parameter.name": "no2",
                "latest.value": 22,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            }
        ]
    }
    city = "test_city"
    location = "test_location"

    df = normalize_data(json_data, city, location)

    assert isinstance(df, pd.DataFrame)
    assert {"city", "location", "date", "time", "pm10", "pm25", "no2", "o3"}.issubset(df.columns)
    assert df["city"].iloc[0] == city
    assert df["location"].iloc[0] == location


# Empty DataFrame.
def test_normalize_data_empty_df():
    json_data = {"results" : []}
    result = normalize_data(json_data, "city", "location")
    assert result is None


# Df is not a list.
def test_normalize_data_not_list():
    json_data = {"results" : "mot a list"}
    result = normalize_data(json_data, "city", "location")
    assert result is None


# Df has no results.
def test_normalize_data_no_results():
    json_data = {}
    result = normalize_data(json_data, "city", "location")
    assert result is None


# Df has no required params.
def test_normalize_data_no_req_params():
    json_data = {
        "results" : [
            {
                "parameter.name" : "co2",
                "latest.value": 13,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            }
        ]
    }

    df = normalize_data(json_data, "city", "location")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


# Date and time are correctly converted.
def test_normalize_data_date_time_format():
    json_data = {
        "results" : [
            {
                "parameter.name" : "pm25",
                "latest.value": 13,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            }
        ]
    }

    df = normalize_data(json_data, "city", "location")

    assert df["date"].iloc[0] == "15:07:2025"
    assert df["time"].iloc[0] == "12:00:00"


# Checking pivoting.
def test_normalize_data_pivot():
    json_data = {
        "results" : [
            {
                "parameter.name" : "pm25",
                "latest.value": 13,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            },
            {
                "parameter.name": "pm10",
                "latest.value": 7,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            },
            {
                "parameter.name": "o3",
                "latest.value": 17,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            },
            {
                "parameter.name": "no2",
                "latest.value": 8,
                "latest.datetime.utc": "2025-07-15T12:00:00Z"
            }
        ]
    }

    df = normalize_data(json_data, "city", "location")

    assert "pm25" in df.columns
    assert "pm10" in df.columns
    assert "o3" in df.columns
    assert "no2" in df.columns
    assert df["pm25"].iloc[0] == 13
    assert df["pm10"].iloc[0] == 7
    assert df["o3"].iloc[0] == 17
    assert df["no2"].iloc[0] == 8


# === Testing save_to_file ===

# Successful save.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_success(mock_storage_client):
    df = pd.DataFrame({
        "city" : ["cityA"],
        "location" : ["locA"],
        "pm25" : [7]
    })

    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    result = save_to_file(df, "bucket_name", "test.csv")

    mock_storage_client.assert_called_once()
    mock_bucket.blob.assert_called_once_with("test.csv")
    mock_blob.upload_from_string.assert_called_once()
    assert result == "gs://bucket_name/test.csv"


# IOError - conversion error.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_conv_error(mock_storage_client):
    class BadDF(pd.DataFrame):
        def to_csv(self, *args, **kwargs):
            raise IOError("Conversion failed")

    bad_df = BadDF()

    result = save_to_file(bad_df, "bucket_name", "test.csv")
    assert result is None


# Storage error.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_stor_error(mock_storage_client):
    df = pd.DataFrame({"city" : ["cityA"]})

    mock_storage_client.side_effect = Exception("Storage init error")

    result = save_to_file(df, "bucket_name", "test.csv")
    assert result is None


# GCS Error for storage.Client().
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_gcs_error_storage(mock_storage_client):
    df = pd.DataFrame({"city" : ["cityA"]})

    mock_storage_client.side_effect = GoogleCloudError("Storage init error")

    result = save_to_file(df, "bucket_name", "test.csv")
    assert result is None


# upload_from_string() returns IOError.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_upload_from_string_error(mock_client):
    df = pd.DataFrame({"city" : ["cityA"]})

    mock_blob = MagicMock()
    mock_blob.upload_from_string.side_effect = IOError("Upload failed")

    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    mock_storage = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_client.return_value = mock_storage

    result = save_to_file(df, "bucket_name", "test.csv")
    assert result is None


# upload_from_string() returns GCSError.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_upload_from_string_gcs_error(mock_client):
    df = pd.DataFrame({"city" : ["cityA"]})

    mock_blob = MagicMock()
    mock_blob.upload_from_string.side_effect = GoogleCloudError("GCS write error")

    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    mock_storage = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_client.return_value = mock_storage

    result = save_to_file(df, "bucket_name", "test.csv")
    assert result is None


# Saving empty df.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_empty_df(mock_client):
    df = pd.DataFrame()

    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    mock_storage = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_client.return_value = mock_storage

    result = save_to_file(df, "bucket_name", "test.csv")

    assert result == "gs://bucket_name/test.csv"
    mock_blob.upload_from_string.assert_called_once()


# Checking arguments of upload_from_string.
@patch("openaq_data_pipeline.api_gcs.storage.Client")
def test_save_to_file_args(mock_client):
    df = pd.DataFrame({"city" : ["cityA"],
                       "location" : ["locA"],
                       "country" : ["PL"]
                       })

    expected_csv = df.to_csv(index = False)

    mock_blob = MagicMock()
    mock_bucket = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    mock_storage = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_client.return_value = mock_storage

    result = save_to_file(df, "bucket_name", "test.csv")

    mock_blob.upload_from_string.assert_called_once_with(
        expected_csv,
        content_type = "text/csv"
    )
    assert result == "gs://bucket_name/test.csv"



# === Testing run ===

# Successful action of the function.
@patch.dict(os.environ, {"OPENAQ_API_KEY" : "test_key", "GCS_BUCKET_NAME" : "test_bucket"}, clear = True)
@patch("openaq_data_pipeline.api_gcs.fetch_data")
@patch("openaq_data_pipeline.api_gcs.normalize_data")
@patch("openaq_data_pipeline.api_gcs.save_to_file")
def test_run_success(mock_save, mock_normalize, mock_fetch):

    mock_fetch.return_value = {"results" :
                                   [{"parameter.name" : "pm25",
                                             "latest.value" : 7,
                                             "latest.datetime.utc" : "2025-07-15T12:12:12Z"}
                                    ]}

    mock_normalize.return_value = pd.DataFrame({"city" : ["cityA"],
                                                "location" : ["locA"],
                                                "date" : ["15:07:2025"],
                                                "time" : ["12:12:12"],
                                                "pm25" : [7]
                                                })
    importlib.reload(api_gcs)

    api_gcs.locations = {"https://openaqurl" : ["cityA", "locA"]}

    mock_save.return_value = "gs://test_bucket/test.csv"

    response = api_gcs.run(None)

    assert response == "File uploaded to gs://test_bucket/test.csv"


# No API key.
@patch.dict(os.environ, {})
def test_run_no_api_key():
    with pytest.raises(EnvironmentError):
        run(None)


# No data collected.
@patch.dict(os.environ, {"OPENAQ_API_KEY" : "test_key"})
@patch("openaq_data_pipeline.api_gcs.fetch_data")
@patch("openaq_data_pipeline.api_gcs.normalize_data")
def test_run_no_data_collected(mock_fetch, mock_normalize):
    mock_fetch.return_value = None
    mock_normalize.return_value = None

    response = run(None)
    assert response == "No data collected"


# No bucket name.
@patch.dict(os.environ, {"OPENAQ_API_KEY" : "test_key", "GCS_BUCKET_NAME" : ""})
@patch("openaq_data_pipeline.api_gcs.fetch_data")
@patch("openaq_data_pipeline.api_gcs.normalize_data")
def test_run_no_bucket_name(mock_fetch, mock_normalize):
    mock_fetch.return_value = {"results" : [{"parameter.name" : "pm25",
                                             "latest.value" : 11,
                                             "latest.datetime.utc" : "2025-07-15T12:12:12Z"
                                             }]}

    mock_normalize.return_value = pd.DataFrame()
    response = run(None)
    assert  response == "GCS Bucket Name not set. Failed to upload the file to GCS."