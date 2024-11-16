FROM apache/airflow:2.6.2-python3.10

USER airflow

# ETL Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# (Updated Nove 2024) ML Code Dependencies
COPY code/models/requirements.txt requirements-models.txt
RUN pip install --no-cache-dir -r requirements-models.txt

# deprecated
# COPY credentials/kaggle.json /home/airflow/.kaggle/
# COPY credentials/gcs_credentials.json .
# COPY credentials/ecomm-invoice-kde-aws-iam_accessKeys.csv .