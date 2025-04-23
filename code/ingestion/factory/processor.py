from abstract.processor import AbstractIngestionProcessor
from factory.config_service import ConfigService
from factory.input_service import InputService
from factory.transform_service import TransformService
from factory.output_service import OutputService

from utils.logging import set_logger

class IngestionProcessor(AbstractIngestionProcessor):
    """
    Orchestrator for Ingestion Processes, Acted as an entrypoint.
    """
    def __init__(
            self,
            config_instance: ConfigService,
            input_instance: InputService,
            transform_instance: TransformService,
            output_instance: OutputService
        ):
        super().__init__()
        self.logger = set_logger(__class__.__name__)

        self.config_instance = config_instance
        self.input_instance = input_instance
        self.transform_instance = transform_instance
        self.output_instance = output_instance

    def process(self):
        self.logger.info("Starting process")
        
        self.config_instance.process()
        
        self.input_instance.get_config(config_instance=self.config_instance)
        self.input_instance.process()
        
        self.transform_instance.get_config(config_instance=self.config_instance)
        self.transform_instance.get_data(df=self.input_instance.df)
        self.transform_instance.process()

        self.output_instance.get_config(config_instance=self.config_instance)
        self.output_instance.get_data(df=self.transform_instance.transformed_df)
        self.output_instance.process()

        self.logger.info("Processes are completed")
