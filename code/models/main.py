import argparse
import logging

from io_services import InputProcessor, OutputProcessor
from ml_services import MLProcessor


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
        "--project_id", type=str, help="", required=False
    )
    parser.add_argument(
        "--method", type=str, help="", required=True
    )
    parser.add_argument(
        "--exec_date", type=str, help="execution date in format 'YYYY-mm-dd'", required=True
    )
    parser.add_argument(
        "--input_path", type=str, help="", required=True # required for filesystem
    )
    parser.add_argument(
        "--output_path", type=str, help="", required=True # required for filesystem
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
    try:
        logging.info("Main Process -- Start")

        # input service
        logging.info("Main Process -- Executing... InputProcessor")
        
        input_processor = InputProcessor(
            env=args.env,
            method=args.method,
            project_id=args.project_id,
            exec_date=args.exec_date,
            input_path=args.input_path,
            output_path=args.output_path
        )
        inputs: dict = input_processor.process()
        df = inputs.get("df")
        interpreter = inputs.get("interpreter")
        
        logging.info("Main Process -- Success: InputProcessor")
        
        # ml services
        logging.info("Main Process -- Executing... MlProcessor")
        
        ml_processor = MLProcessor(
            df=df, 
            interpreter=interpreter,
            force_train=args.force_train
        )
        outputs: dict = ml_processor.process()
        
        logging.info("Main Process -- Success: MlProcessor")

        # output service
        logging.info("Main Process -- Executing... OutputProcessor")
        
        output_processor = OutputProcessor(
            env=args.env,
            method=args.method,
            project_id=args.project_id,
            output_path=args.output_path,
            exec_date=args.exec_date,
            outputs=outputs
        )
        output_processor.process()
        
        logging.info("Main Process -- Success: OutputProcessor")

        logging.info("Main Process -- Success")
    except Exception as err:
        logging.exception(f"Unexpected {err}, {type(err)}")
        raise


if __name__ == "__main__":
    entrypoint()
