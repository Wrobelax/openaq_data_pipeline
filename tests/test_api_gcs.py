"""Testing api_gcs script by using pytest"""
import logging

# Importing modules.
import pytest
from unittest.mock import patch, MagicMock

import requests.exceptions

from openaq_data_pipeline.api_gcs import fetch_data, normalize_data, save_to_file, run
from requests.exceptions import HTTPError


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
