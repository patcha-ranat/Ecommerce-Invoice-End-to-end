FROM python:3.10.15-slim

# For properly install lightgbm dependency
RUN apt-get update && \
    apt-get -y install curl &&\
    apt-get install libgomp1

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt --no-cache

ENTRYPOINT ["python", "main.py"]
# CMD ["bash"]