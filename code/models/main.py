import argparse
import logging

from io_services import InputProcessor, OutputProcessor
from ml_services import MlProcessor


# set basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

def entrypoint():
    parser = argparse.ArgumentParser(
        description="AI-Driven Interpretable Dynamic Customer Segmentation"
    )
    parser.add_argument(
        "--env", type=str, help="", required=True
    )
    parser.add_argument(
        "--method", type=str, help="", required=True
    )
    parser.add_argument(
        "--input_path", type=str, help="", required=True
    )
    parser.add_argument(
        "--output_path", type=str, help="", required=True
    )
    parser.add_argument(
        "--exec_date", type=str, help="execution date in format 'YYYY-mm-dd'", required=True
    )
    parser.add_argument(
        "--force_train", action="store_true"
    )

    args, unknown_args = parser.parse_known_args()
    # args = parser.parse_args() # get only known args

    if len(unknown_args) != 0:
        logging.warning(f"Unknow arguments: {unknown_args}")

    logging.info(f"Input argument: {args}")

    # process

    logging.info("Main Process -- Start")

    # input service
    logging.info(f"Main Process -- Executing... InputProcessor")
    
    input_processor = InputProcessor(
        env=args.env,
        method=args.method, 
        input_path=args.input_path,
        output_path=args.output_path
    )
    inputs: dict = input_processor.process()
    df = inputs.get("df")
    interpreter = inputs.get("interpreter")
    
    logging.info(f"Main Process -- Success: InputProcessor")
    
    # ml services
    logging.info(f"Main Process -- Executing... MlProcessor")
    
    ml_processor = MlProcessor(
        df=df, 
        interpreter=interpreter,
        force_train=args.force_train
    )
    outputs: dict = ml_processor.process()
    
    logging.info(f"Main Process -- Success: MlProcessor")

    # output service
    logging.info("Main Process -- Executing... OutputProcessor")
    
    output_processor = OutputProcessor(
        env=args.env,
        method=args.method,
        output_path=args.output_path,
        date=args.exec_date,
        outputs=outputs
    )
    output_processor.process()
    
    logging.info("Main Process -- Success: OutputProcessor")

    logging.info("Main Process -- Success")


if __name__ == "__main__":
    entrypoint()
