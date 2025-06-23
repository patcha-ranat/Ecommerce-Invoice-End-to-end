# This file is never meant to be executed as a script.
# Except, for being demonstration of how commands can be execute for this project.

# Setting up Python Environment and Poetry
export POETRY_HOME="/c/Users/<your-pc-user>/.poetry-env"
# export POETRY_HOME="~/.poetry-env"
export VENV_PATH="${POETRY_HOME}/Scripts"
export PATH="${VENV_PATH}:${PATH}"

python -m venv ~/.poetry-env

# Preparing Files for ELT processes
gsutil cp data/uncleaned_data.csv gs://<landing_bucket>/ecomm_transaction/2025-05-12/uncleaned_data.csv
gsutil cp notebooks/gcs-connector-3.0.7-shaded.jar gs://<artifact_bucket>/gcs-connector-3.0.7-shaded.jar

# Compressed Pyspark Ingestion Framework as a mudule
# using subshell to avoid changing directory of the current shell
(cd code/ingestion/ && zip -r ../../tempdir/ecomm_ingestion_framework.zip . -x "*__pycache__/*" "example/*")

# Copy the Compressed Pyspark Ingestion Framework module to GCS
gsutil cp tempdir/ecomm_ingestion_framework.zip gs://kde_ecomm_artifact/ecomm_ingestion_framework.zip