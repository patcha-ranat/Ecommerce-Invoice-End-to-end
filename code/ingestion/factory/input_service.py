from pyspark.sql import SparkSession, dataframe

from abstract.input_service import AbstractInputService
from factory.config_service import ConfigService
from utils.logging import set_logger


class InputService(AbstractInputService):
    """
    Ingestion Reader for Data File
    """
    def __init__(
            self, 
            spark: SparkSession, 
            input_path: str,
            read_format: str
        ):
        """
        Class Entrypoint
        """
        super().__init__()
        self.logger = set_logger(__class__.__name__)

        # Instance Input
        self.spark = spark
        self.input_path = input_path
        self.read_format = read_format

        # Service Input
        self.config_instance: ConfigService = None
        
        # output
        self.df: dataframe.DataFrame

    def get_config(self, config_instance) -> None:
        """
        Adding Config after instance initiated
        """
        self.logger.debug("Adding ConfigService to InputService")
        self.config_instance = config_instance

    def read_data(self) -> None:
        """
        Reading Data with Configuration, regardless type of filesystem (local, gcp, aws) which is managed in dependency level. 
        """
        self.logger.debug(f"Reading data from '{self.input_path}'")
        df = self.spark.read\
            .schema(self.config_instance.source_schema)\
            .format(self.read_format)\
            .load(self.input_path)
        
        self.df = df

    def process(self):
        """
        Class Main Process
        """
        self.logger.info("Staring process")
        self.read_data()
