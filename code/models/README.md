# Interpretable Dynamic Customer Segmentation
*with RFM, KMeans, and LightGBM*

*patcharanat p.*

## Containerized with Docker
```bash
# workdir code/models

# docker build -t <image_name:tag> .
docker build -t ecomm/interpretable-dynamic-rfm-service:v1 .

# docker run --name <container_name> <image_name:tag> <app_args1> ...

# --rm: to remove container after runtime

# copy data/ecomm_invoice_transaction.parquet to code/models

# with python main.py as an entrypoint
docker run --rm \
    ecomm/interpretable-dynamic-rfm-service:v1 \
    --env local --method filesystem \
    --input_path 'ecomm_invoice_transaction.parquet' \
    --output_path output --exec_date 2024-10-29

# with bash as en entrypoint (cmd)
docker run --rm -it ecomm/interpretable-dynamic-rfm-service:v1
python main.py ...
```

## Usage