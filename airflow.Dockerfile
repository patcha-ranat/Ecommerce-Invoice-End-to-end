FROM apache/airflow:2.6.2-python3.10

# install gcloud and set to PATH
WORKDIR /gcp
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-arm.tar.gz \
    && tar -xf google-cloud-cli-linux-arm.tar.gz \
    && rm google-cloud-cli-linux-arm.tar.gz
RUN ./google-cloud-sdk/install.sh
ENV PATH=$PATH:/gcp/google-cloud-sdk/bin

WORKDIR /opt/airflow

# ETL Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# (Updated Nov 2024) ML Code Dependencies
COPY code/models/requirements.txt requirements-models.txt
RUN pip install --no-cache-dir -r requirements-models.txt
