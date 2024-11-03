# cd ./code/models

# local
python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-11-03

# local force train
python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-10-29 --force_train

# gcp
python main.py --env gcp --project_id <project_id> --method filesystem --input_path 'gs://<landing_bucket_name>/input/data/2024-11-03/ecomm_invoice_transaction.parquet' --output_path 'gs://<staging_bucket_name>/output' --exec_date 2024-11-03
