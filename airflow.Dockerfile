FROM apache/airflow:latest

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY kaggle.json .

COPY kaggle.json /home/airflow/.kaggle/