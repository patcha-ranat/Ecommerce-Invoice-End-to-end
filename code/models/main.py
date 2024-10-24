import argparse
import logging

from io_services import InputProcessor, OutputProcessor
from ml_services import MlProcessor


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
    # args = parser.parse_args()

    if len(unknown_args) != 0:
        logging.warning(f"Unknow arguments: {unknown_args}")

    logging.info(f"Input argument: {args}")

    # process
    # input
    input_processor = InputProcessor(
        env=args.env,
        method=args.method, 
        input_path=args.input_path
    )
    df = input_processor.process()
    
    # ml services
    ml_processor = MlProcessor(df=df)
    output: dict = ml_processor.process()

    # output
    output_processor = OutputProcessor(
        env=args.env,
        method=args.method,
        output_path=args.output_path,
        output=output
    )

    output_processor.process()

if __name__ == "__main__":
    entrypoint()
