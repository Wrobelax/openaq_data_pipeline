![Pytest](https://img.shields.io/badge/project_status-completed/closed-darkgreen)

![Pytest](https://img.shields.io/badge/tests-completed/passed-darkgreen)

## **Project description**

This project fetches and processes data from the OpenAQ API and uploads it to Google Cloud Storage.  
The main goal is to automate data retrieval and storage of air quality data in the cloud.

---

### Features

- Fetching data from OpenAQ API
- Data normalization and processing into tabular format
- Saving data as CSV files to Google Cloud Storage
- Configuration via environment variables
- Unit tests using "pytest" with mocks

---

### Tech Stack

- Python 3.11+ (tested on 3.13)
- Pandas
- Google Cloud Storage Client
- Pytest


### Project Structure

- /openaq_data_pipeline
  - tests/                # Unit tests
    - \_\_init__.py
    - pytest_log.txt
    - test_api_gcs.py     # Script with tests
  - \_\_init__.py
  - api_gcs.py            # Main logic for fetching, processing, and uploading data
  - README.md             # Project documentation


### How to Run Tests

1. Install required packages (e.g., `pip install -r requirements.txt` if available).
2. Run tests with:
   ```bash
   pytest
   
### Setting up environment variables for the script
To run the script, it is necessary to provide OpenAQ API key and Google Cloud Storage (GCS) bucket name without hardcoding them directly into the Python file. This is done by setting them as environment variables in the environment where the script will execute.

Here are instructions for common operating systems and deployment scenarios:

1. On Linux / macOS (Bash / Zsh):
Open terminal.

Set the variables using export:

```bash
export OPENAQ_API_KEY="YOUR_OPENAQ_API_KEY_HERE"
export GCS_BUCKET_NAME="YOUR_GCS_BUCKET_NAME_HERE"
```

Replace "YOUR_OPENAQ_API_KEY_HERE" and "YOUR_GCS_BUCKET_NAME_HERE" with your actual key and bucket name.

Run Python script:

```bash
python your_script_name.py
```

2. On Windows (Command Prompt cmd):
Open Command Prompt.

Set the variables using set:

```dos
set OPENAQ_API_KEY=YOUR_OPENAQ_API_KEY_HERE
set GCS_BUCKET_NAME=YOUR_GCS_BUCKET_NAME_HERE
```

Replace YOUR_OPENAQ_API_KEY_HERE and YOUR_GCS_BUCKET_NAME_HERE with your actual key and bucket name.

Run your Python script:

```dos
python your_script_name.py
```

3. For Google Cloud Functions
When deploying to Google Cloud Functions, you configure environment variables directly during deployment.

When deploying your function (either via the Cloud Console UI or gcloud CLI), look for the "Runtime environment variables" or "Environment variables" section.

Add two new variables:

Name: OPENAQ_API_KEY, Value: YOUR_OPENAQ_API_KEY_HERE

Name: GCS_BUCKET_NAME, Value: YOUR_GCS_BUCKET_NAME_HERE

Using the gcloud CLI:

```bash
gcloud functions deploy YOUR_FUNCTION_NAME \
  --runtime python39 \
  --trigger-http \
  --entry-point run \
  --set-env-vars OPENAQ_API_KEY="YOUR_OPENAQ_API_KEY_HERE",GCS_BUCKET_NAME="YOUR_GCS_BUCKET_NAME_HERE" \
```
