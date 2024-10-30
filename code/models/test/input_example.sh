# cd ./code/models
python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-10-29

# force train
python main.py --env local --method filesystem --input_path '../../data/ecomm_invoice_transaction.parquet' --output_path output --exec_date 2024-10-29 --force_train