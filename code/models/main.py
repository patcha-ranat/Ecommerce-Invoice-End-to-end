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

    args, unknown_args = parser.parse_known_args()
    # args = parser.parse_args() # get only known args

    if len(unknown_args) != 0:
        logging.warning(f"Unknow arguments: {unknown_args}")

    logging.info(f"Input argument: {args}")

    # process
    # input service
    logging.info(f"{'-'*50} Executing... InputProcessor {'-'*50}")
    
    input_processor = InputProcessor(
        env=args.env,
        method=args.method, 
        input_path=args.input_path,
        output_path=args.output_path
    )
    inputs: dict = input_processor.process()
    df = inputs.get("df")
    interpreter = inputs.get("interpreter")
    
    logging.info(f"{'-'*50} Success: InputProcessor {'-'*50}")
    
    # ml services
    logging.info(f"{'-'*50} Executing... MlProcessor {'-'*50}")
    
    ml_processor = MlProcessor(df=df, interpreter=interpreter)
    outputs: dict = ml_processor.process()
    
    logging.info(f"{'-'*50} Success: MlProcessor {'-'*50}")

    # output service
    logging.info(f"{'-'*50} Executing... OutputProcessor {'-'*50}")
    
    output_processor = OutputProcessor(
        env=args.env,
        method=args.method,
        output_path=args.output_path,
        outputs=outputs
    )
    output_processor.process()
    
    logging.info(f"{'-'*50} Success: OutputProcessor {'-'*50}")


if __name__ == "__main__":
    entrypoint()
