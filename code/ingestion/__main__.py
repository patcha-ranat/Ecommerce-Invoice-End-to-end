import argparse
import json

from pyspark.sql import SparkSession

from factory.argument_service import ArgumentService
from factory.config_service import ConfigService
from factory.input_service import InputService
from factory.transform_service import TransformService
from factory.output_service import OutputService
from factory.processor import IngestionProcessor

from utils.logging import set_logger


def entrypoint():
    logger = set_logger("Driver")
    app_name = "PySpark Ingestion Framework"

    parser = argparse.ArgumentParser(app_name)

    parser.add_argument(
        "--spark-config", type=json.loads, required=False,
        help="string of dict of config where config is python dict configuration"
    )

    parser.add_argument(
        "--spark-log-level", type=str, choices=["INFO", "ERROR"], default="ERROR",
        help="parse the argument if logs inspectation is required"
    )

    parser.add_argument(
        "--ingestion-config", type=json.loads, required=True, 
        help="String of dict of ingestion configuration"
    )

    parser.add_argument(
        "--input-path", type=str, required=True, 
        help="Path to input file, can be either bucket+path or filesystem path"
    )

    parser.add_argument(
        "--output-path", type=str, required=True, 
        help="Path to output file, can be either bucket+path or filesystem path"
    )

    parser.add_argument(
        "--dt", type=str, required=False, 
        help="Execution date to filter"
    )

    parser.add_argument(
        "--write-mode", type=str, required=False, default="overwrite", choices=["overwrite", "append"], 
        help="Replace or delta, can be replaced by partition by config partition column"
    )

    parser.add_argument(
        "--read-format", type=str, required=False, default="csv", choices=["csv", "json", "txt"], 
        help="Input read format"
    )
    
    parser.add_argument(
        "--write-format", type=str, required=False, default="parquet", choices=["parquet", "delta"], 
        help="Output write format"
    )

    parser.add_argument(
        "--ingestion-mode", type=str, required=False, default="batch", choices=["batch", "streaming"], 
        help="Spark streaming/batch mode"
    )

    args = parser.parse_args()

    logger.info("Read arguments successfully. Starting Ingestion Framework.")

    try:
        ArgumentService(args=args).process()
        
        # start spark session
        spark = SparkSession.builder \
                    .master("local[*]") \
                    .appName(app_name) \
                    .config("spark.driver.memory", "4g") \
                    .getOrCreate()
        # supress spark log
        spark.sparkContext.setLogLevel(args.spark_log_level)

        # start ingestion process
        config_instance = ConfigService(
            ingestion_config=args.ingestion_config
        )

        input_instance = InputService(
            spark=spark, 
            input_path=args.input_path, 
            read_format=args.read_format
        )

        transform_instance = TransformService(
            execution_date=args.dt
        )

        output_instance = OutputService(
            output_path=args.output_path,
            write_mode=args.write_mode,
            write_format=args.write_format
        )

        # AuditService
        
        processor_instance = IngestionProcessor(
            config_instance=config_instance,
            input_instance=input_instance,
            transform_instance=transform_instance,
            output_instance=output_instance
        )

        processor_instance.process()

        logger.info("Ingestion Framework executed successfully.")
    
    except Exception as err:
        logger.error("Ingestion Framework executed unsuccessfully.")
        raise err
    
    finally:
        spark.stop()


if __name__ == '__main__':

    entrypoint()