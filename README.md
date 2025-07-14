![Pytest](https://img.shields.io/badge/project_status-in_progress\open-blue)

![Pytest](https://img.shields.io/badge/tests-pending-blue)

## **Project description**


### Features


### Tech Stack
- Python 3.11+ (used 3.13)


### Project structure


### How to run tests


### Setting up environment variables for the script
To run the script, necessary is to provide OpenAQ API key and Google Cloud Storage (GCS) bucket name without hardcoding them directly into Python file. This is done by setting them as environment variables in the environment where the script will execute.

Here are instructions for common operating systems and deployment scenarios:

1. On Linux / macOS (Bash / Zsh):
Open terminal.

Set the variables using export:

Bash

export OPENAQ_API_KEY="YOUR_OPENAQ_API_KEY_HERE"
export GCS_BUCKET_NAME="YOUR_GCS_BUCKET_NAME_HERE"
Replace "YOUR_OPENAQ_API_KEY_HERE" and "YOUR_GCS_BUCKET_NAME_HERE" with your actual key and bucket name.

Run Python script:

Bash

python your_script_name.py

2. On Windows (Command Prompt cmd):
Open Command Prompt.

Set the variables using set:

DOS

set OPENAQ_API_KEY=YOUR_OPENAQ_API_KEY_HERE
set GCS_BUCKET_NAME=YOUR_GCS_BUCKET_NAME_HERE
Replace YOUR_OPENAQ_API_KEY_HERE and YOUR_GCS_BUCKET_NAME_HERE with your actual key and bucket name.

Run your Python script:

DOS

python your_script_name.py

3. For Google Cloud Functions
When deploying to Google Cloud Functions, you configure environment variables directly during deployment.

When deploying your function (either via the Cloud Console UI or gcloud CLI), look for the "Runtime environment variables" or "Environment variables" section.

Add two new variables:

Name: OPENAQ_API_KEY, Value: YOUR_OPENAQ_API_KEY_HERE

Name: GCS_BUCKET_NAME, Value: YOUR_GCS_BUCKET_NAME_HERE

Using the gcloud CLI:

Bash

gcloud functions deploy YOUR_FUNCTION_NAME \
  --runtime python39 \
  --trigger-http \
  --entry-point run \
  --set-env-vars OPENAQ_API_KEY="YOUR_OPENAQ_API_KEY_HERE",GCS_BUCKET_NAME="YOUR_GCS_BUCKET_NAME_HERE" \
  # Add other deployment options as needed (e.g., --source, --region)