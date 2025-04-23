from pyspark.sql import dataframe

from abstract.output_service import AbstractOutputService
from factory.config_service import ConfigService

from utils.logging import set_logger


class OutputService(AbstractOutputService):
    """
    Ingestion Data File Writer
    """
    def __init__(
            self, 
            output_path: str,
            write_mode: str,
            write_format: str
        ):
        """
        """
        super().__init__()
        self.logger = set_logger(__class__.__name__)

        # input
        self.output_path =  output_path
        self.write_mode = write_mode
        self.write_format = write_format

        # runtime input
        self.df: dataframe.DataFrame
        self.config_instance: ConfigService

    def get_config(self, config_instance: ConfigService) -> None:
        """
        Get ingestion config from ConfigService for partition writing
        """
        self.config_instance = config_instance
    
    def get_data(self, df: dataframe.DataFrame) -> None:
        """
        Get Transformed Data from TransformService
        """
        self.df = df
    
    def get_partition(self):
        """
        Retrieve the specified partition column without date partition
        """
        existing_partitions: list = self.config_instance.partition_column.copy()
        existing_partitions.remove(self.config_instance.transaction_date_col_name)
        return existing_partitions

    def write(self):
        """
        Writing Data with Configuration
        """
        if "gs://" in self.output_path:
            self.logger.info("Writing data to GCS")
            pass

        elif "s3://" in self.output_path:
            self.logger.info("Writing data to S3")
            pass
        
        else: # Local filesystem
            self.logger.warning("Reading data from Local filesystem, this is for testing.")
            self.df.write \
                    .partitionBy("ptn_yyyy", "ptn_mm", "ptn_dd", *self.get_partition()) \
                    .mode(self.write_mode) \
                    .format(self.write_format) \
                    .parquet(self.output_path)

    def process(self):
        """
        Class Main Process
        """
        self.write()