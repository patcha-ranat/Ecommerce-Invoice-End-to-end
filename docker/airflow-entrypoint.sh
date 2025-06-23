#!/bin/bash

airflow variables import config/variables.json

airflow connections import config/connections.json --overwrite

gcloud auth application-default print-access-token | docker login -u oauth2accesstoken --password-stdin https://${GCP_REGION}-docker.pkg.dev

exec /entrypoint "${@}"