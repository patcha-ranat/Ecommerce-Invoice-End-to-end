FROM ubuntu:22.04

RUN apt update

RUN apt install -y python3 python3-pip -y

WORKDIR /app

RUN pip install fastapi uvicorn pydantic

COPY ./deployment .

# we can omit EXPOSE to port if we define in it somewhere else
# EXPOSE 80

CMD ["uvicorn", "api_app:app", "--host", "0.0.0.0", "--port", "80"]

# execute the following command to build the image at parent directory
# docker build -t ecomm-invoice-api-local -f deployment\deployment.Dockerfile .
# even it said it's built at 0.0.0.0:80, access via localhost:80