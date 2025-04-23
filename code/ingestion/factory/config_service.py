from typing import Any

from abstract.config_service import AbstractConfigService
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, TimestampType, DecimalType
from utils.logging import set_logger


class ConfigService(AbstractConfigService):
    """
    Read Ingestion Configuration for Input Reading and Transformation processes.
    """
    def __init__(self, ingestion_config: dict):
        """
        Class Entrypoint
        """
        super().__init__()
        self.logger = set_logger(__class__.__name__)
        
        # input
        self.ingestion_config: dict[str, Any] = ingestion_config

        # output schemas
        self.source_schema: StructType
        self.target_schema: list[dict[str, Any]]

        # output attributes
        self.source_dataset: str
        self.target_dataset: str
        self.table_name: str
        self.primary_key: list
        self.partition_column: list
        self.transaction_date_col_name: str
        # self.is_array_column_exist: bool
        # self.array_column: list

        # default attributes
        self.default_source_dataset: str = "bronze"
        self.default_target_dataset: str = "silver"

    def validate(self):
        """
        Validate schema config
        """
        self.logger.info("Starting Schema Configuration Validation.")
        
        attributes = self.ingestion_config.keys()
        
        # validation conditions
        if "dataset" not in attributes:
            self.logger.warning("'dataset' is not spcecified, default datasets will be applied for target table.")
            datasets = self.ingestion_config.get("dataset").keys()
            if "source" not in datasets:
                self.logger.warning(f"'dataset.source' is not specified, default dataset: '{self.default_source_dataset}' will be applied.")
            if "target" not in datasets:
                self.logger.warning(f"'dataset.target' is not specified, default dataset: '{self.default_target_dataset}' will be applied.")
        
        if "table_name" not in attributes:
            raise Exception("'table_name' attribute is not in the configuration file.")
        
        if "columns" not in attributes:
            raise Exception("'columns' attribute is not in the configuration file.")
        
        if "primary_key" not in attributes:
            raise Exception("'primary_key' is not specified. It should be specified as a blank list atleast.")
        elif len(self.ingestion_config.get("primary_key")) == 0:
            self.logger.warning("'primary_key' is not specified, de-duplication will be affected")
            # other validation logic
        
        if "partition_column" not in attributes:
            raise Exception("'partition_column' is not specified. It should be specified as a blank list atleast.")
        else:
            partition_column = self.ingestion_config.get("partition_column") 
            if len(partition_column) == 0:
                self.logger.warning("'partition_column' is not specified, partitioning will be affected")
            else: # len('partition_column') > 0
                # Check if date column specified to be partitions
                date_columns = [col_name for col_name in partition_column if "date" in col_name]
                if len(date_columns) > 1:
                    raise Exception("Multiple date columns used in partition in which the framework does not accept.")
                else: # len(date_columns) == 1
                    self.transaction_date_col_name = date_columns[0]
           # other validation logic


        if "array_column" not in attributes:
            self.logger.debug("'array_column' is not specified")
            self.is_array_column_exist = False
        else:
            self.is_array_column_exist = True

        # validate column level
        for column in self.ingestion_config.get("columns"):
            column_attributes: list = column.keys()

            if "target_column" not in column_attributes:
                raise Exception("'target_column' is not specified")
            if "source_column" not in column_attributes:
                raise Exception("'source_column' is not specified")
            if "column_type" not in column_attributes:
                raise Exception("'column_type' is not specified")
            if "nullable" not in column_attributes:
                raise Exception("'nullable' is not specified")
            
            # validate transformation rules level
            if column.get("transformation_rules"):
                column_type = column.get("column_type")
                transformation_rules: list[str] = []
                for transformation_rule in column.get("transformation_rules"):
                    transformation_rules += transformation_rule.keys()

                if (column_type == "str") and ("round" in transformation_rules):
                    raise Exception("Data Type: str can't be rounded")
                # other validation logic
            
        self.logger.info("Validation Succeeded.")
        
    def generate_schema(self) -> None:
        """
        Generating source and target schemas according to ingestion configuration.
        Return None, instead updating class attributes.
        """
        self.logger.info("Starting Generating Schemas")

        schema_mapping: dict = {
            "str": StringType(),
            "int": IntegerType(),
            "float": DecimalType(),
            "bool": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType()
        }

        # Map config to schema
        raw_schema: list = [] # used for input reading
        target_schema: list = [] # used for logging
        
        for column in self.ingestion_config.get("columns"):
            # create raw schema
            raw_schema.append(
                StructField(name=column.get("source_column"), dataType=StringType(), nullable=column.get("nullable"))
            )

            # create target schema
            target_schema.append(
                {
                    "target_column": column.get("target_column"),
                    "source_column": column.get("source_column"),
                    "column_type": schema_mapping.get(column.get("column_type")), 
                    "nullable": column.get("nullable"),
                    "transformation_rules": column.get("transformation_rules")
                }
            )

        # create target schema, if new columns specified
        if self.ingestion_config.get("new_columns"):
            for column in self.ingestion_config.get("new_columns"):
                target_schema.append(
                    {
                        "target_column": column.get("target_column"),
                        "source_column": column.get("source_column"),
                        "column_type": schema_mapping.get(column.get("column_type")), 
                        "nullable": column.get("nullable"),
                        "transformation_rules": column.get("transformation_rules")
                    }
                )
        
        self.source_schema = StructType(raw_schema)
        self.target_schema = target_schema

    def generate_patition_attribute(self, partition_columns: list[str]):
        """
        Generating Partition Attribute: 'transaction_date_col_name' that will be used as partition column
        """
        self.logger.info("Starting Generating Partition Attribute")

        if len(partition_columns) > 0:
            # Check if date column specified to be partitions
            date_columns = [col_name for col_name in partition_columns if "date" in col_name]
            
            # len(date_columns) must == 1, unless shouldn't pass validation
            self.transaction_date_col_name = date_columns[0]
        else:
            self.transaction_date_col_name = None

    def generate(self):
        """
        Generate schema from schema config to be used in spark process
        """
        self.logger.info("Starting Generating")
        self.generate_schema()

        self.source_dataset = self.ingestion_config.get("dataset", self.default_source_dataset).get("source", self.default_source_dataset)
        self.target_dataset = self.ingestion_config.get("target_dataset", self.default_target_dataset)
        self.table_name = self.ingestion_config.get("table_name")
        self.primary_key = self.ingestion_config.get("primary_key")
        self.partition_column = self.ingestion_config.get("partition_column")
        
        # Generate partition column attribute
        partition_columns = self.ingestion_config.get("partition_column")
        self.generate_patition_attribute(partition_columns=partition_columns)
        
        # self.array_column = self.ingestion_config.get("array_column")

        self.logger.info("Schema Generation Succeeded.")

    def log(self):
        """
        Logging every output
        """
        self.logger.debug(f"Read Schema Config: {self.ingestion_config}")
        self.logger.info(f"source_schema: {self.source_schema}")
        self.logger.info(f"target_schema: {self.target_schema}")
        self.logger.info(f"source_dataset: {self.source_dataset}")
        self.logger.info(f"target_dataset: {self.target_dataset}")
        self.logger.info(f"table_name: {self.table_name}")
        self.logger.info(f"primary_key: {self.primary_key}")
        self.logger.info(f"partition_column: {self.partition_column}")
        self.logger.info(f"transaction_date_col_name: {self.transaction_date_col_name}")

    def process(self):
        """
        Class Main Process
        """
        self.logger.info("Staring process")
        self.validate()
        self.generate()
        self.log()
