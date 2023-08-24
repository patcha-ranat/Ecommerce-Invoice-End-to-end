FROM apache/airflow:2.6.2-python3.10

USER airflow

# RUN curl -sSL https://install.python-poetry.org | python -

# RUN pip install poetry

# COPY pyproject.toml poetry.lock ./

# RUN poetry install

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY credentials/kaggle.json /home/airflow/.kaggle/

COPY credentials/gcs_credentials.json .

COPY credentials/ecomm-invoice-kde-aws-iam_accessKeys.csv .