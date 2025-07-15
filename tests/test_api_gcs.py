"""Testing api_gcs script by using pytest"""

# Importing modules.
import pytest
from unittest.mock import patch, MagicMock
import requests.exceptions
from openaq_data_pipeline.api_gcs import fetch_data, normalize_data, save_to_file, run
import pandas as pd



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