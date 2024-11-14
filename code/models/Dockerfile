FROM python:3.10.15-slim

# For properly install lightgbm dependency
RUN apt-get update && \
    apt-get -y install curl &&\
    apt-get install libgomp1

# Set working directory within docker container
WORKDIR /app

# Copy local file into the container
COPY . .

# Install Python dependencies
RUN pip install -r requirements.txt --no-cache

# Specify entrypoint of the container
ENTRYPOINT ["python", "main.py"]
# CMD ["bash"]