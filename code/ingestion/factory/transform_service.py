from typing import Any

from pyspark.sql import dataframe
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, TimestampType

from abstract.transform_service import AbstractTransformService
from factory.config_service import ConfigService
from utils.logging import set_logger


class TransformService(AbstractTransformService):
    """
    Applying Transformation from Ingestion Configuration and Enriching data
    """
    def __init__(self, execution_date: str):
        """
        Class Entrypoint
        """
        super().__init__()
        self.logger = set_logger(__class__.__name__)

        # input
        self.dt: str = execution_date

        # runtime input
        self.df: dataframe.DataFrame
        self.config_instance: ConfigService

        # intermediate result
        self.intermediate_df: dataframe.DataFrame
        # save from transformation applying looping for futher enrichment process
        self.source_date_format: str

        # output
        self.transformed_df: dataframe.DataFrame

    def get_config(self, config_instance: ConfigService) -> None:
        """
        Get ingestion config from ConfigService
        """
        self.logger.debug("Parsing ConfigService to TransformService")
        self.config_instance = config_instance

    def get_data(self, df: dataframe.DataFrame) -> None:
        """
        Get Data From InputService
        """
        self.logger.debug("Parsing DataFrame Input to TransformService")
        self.df = df

    def transform(self) -> None:
        """
        Applying Transformation Logic from Ingestion Configuration
        
        Example of 'target_config' or 'target_schema':
        [
            {
                "target_column": "target_column_name"
                "source_column": "source_column_name"
                "column_type": DateType()
                "nullable": False
                "transformation_rules": [
                    {"trim": ["ltrim", "trim"]},
                    {"format_datetime": {"source_format": "yyyy/MM/dd", "target_format": "yyyy-MM-dd"}}
                ]
            },
            ...
        ]
        """
        self.logger.info("Applying Transformation regarding to Ingestion Configuration")
        self.intermediate_df = self.df
        target_config: list[dict[str, Any]] = self.config_instance.target_schema
        
        self.logger.debug("Iterrating over column configs")
        # iterrate over column configs
        for column_config in target_config:
            # assign repeatedly used variable
            target_col_name = column_config.get("target_column")

            self.logger.debug(f"Processing Column: {target_col_name}")

            # create a new target column
            self.logger.debug("Creating the column")
            self.intermediate_df = self.intermediate_df.withColumn(
                target_col_name, 
                F.col(column_config.get("source_column"))
            )
            
            self.logger.debug("Iterrating over transformation rules for the column")
            # iterrate applying transformations if exist
            if column_config.get("transformation_rules"):
                for transformation_rule in column_config.get("transformation_rules"):
                    
                    transformation_rules = transformation_rule.keys()

                    self.logger.debug(f"Column: {target_col_name}, Transformation Rules: {transformation_rules}")
                    
                    if "trim" in transformation_rules:
                        self.logger.debug("Applying 'trim' transformations")

                        for transformation_method in transformation_rule.get("trim"):   
                            if transformation_method == "ltrim":
                                self.intermediate_df = self.intermediate_df.withColumn(
                                    target_col_name, 
                                    F.ltrim(F.col(target_col_name))
                                )
                            if transformation_method == "rtrim":
                                self.intermediate_df = self.intermediate_df.withColumn(
                                    target_col_name, 
                                    F.rtrim(F.col(target_col_name))
                                )
                            if transformation_method == "trim":
                                self.intermediate_df = self.intermediate_df.withColumn(
                                    target_col_name, 
                                    F.trim(F.col(target_col_name))
                                )

                    if "format_datetime" in transformation_rules:
                        self.logger.debug("Applying 'format_datetime' transformations")
                        
                        # save format for enrichment process
                        if target_col_name == self.config_instance.transaction_date_col_name:
                            self.source_date_format = transformation_rule.get("format_datetime").get("source_format")

                        # apply format
                        if isinstance(column_config.get("column_type"), DateType):
                            self.intermediate_df = self.intermediate_df.withColumn(
                                target_col_name, 
                                F.date_format(
                                    F.to_date(
                                        target_col_name, 
                                        self.source_date_format
                                    ),
                                    transformation_rule.get("format_datetime").get("target_format")
                                )
                            )
                        elif isinstance(column_config.get("column_type"), TimestampType):
                            self.intermediate_df = self.intermediate_df.withColumn(
                                target_col_name, 
                                F.date_format(
                                    F.to_timestamp(
                                        target_col_name, 
                                        self.source_date_format
                                    ),
                                    transformation_rule.get("format_datetime").get("target_format")
                                )
                            )
                    
                    if "round" in transformation_rules:
                        self.logger.debug("Applying 'round' transformations")

                        # rounding need to convert type to number type first
                        self.intermediate_df = self.intermediate_df.withColumn(
                            target_col_name,
                            F.col(target_col_name).cast(column_config.get("column_type"))
                        )
                        
                        self.intermediate_df = self.intermediate_df.withColumn(
                            target_col_name, 
                            F.round(
                                F.col(target_col_name), 
                                transformation_rule.get("round")
                            )
                        )
            else:
                self.logger.debug("No Transformation applied.")

            # Cast Type
            self.logger.debug(f"Casting Type: {column_config.get("column_type")}")
            self.intermediate_df = self.intermediate_df.withColumn(
                target_col_name,
                F.col(target_col_name).cast(column_config.get("column_type"))
            )

    def enrich(self) -> None:
        """
        Enriching Data, including:
        1. drop duplicated rows regarding primary keys
        2. create surrogate key: '_kde_id' according to primary keys
        3. create ingestion execution date column: '_ingestion_date'
        4. create partition columns: ptn_yyyy, ptn_mm, ptn_dd
        5. re-arrange & select columnsx
        """
        self.logger.info("Starting Enrichment Processes")
        # 1. drop duplicated
        if len(self.config_instance.primary_key) == 0:
            self.logger.warning("No de-duplication applied due to missing primary keys.")
        else:
            self.transformed_df = self.intermediate_df.dropDuplicates(self.config_instance.primary_key)
        
        # 2. create surrogate key using row_number window
        w = Window.orderBy(F.lit("A")) # orderBy as it is by a dummy value
        self.transformed_df = self.transformed_df.withColumn(
            "_kde_id",
            F.row_number().over(w)
        )

        # 3. create ingestion date
        self.transformed_df = self.transformed_df.withColumn(
            "_ingestion_date",
            F.lit(self.dt).cast(DateType())
        )

        # 4. create partition columns
        if (len(self.config_instance.partition_column) == 0) or (self.config_instance.transaction_date_col_name is None):
            self.transformed_df = self.transformed_df \
                .withColumn("ptn_yyyy", F.year("_ingestion_date")) \
                .withColumn("ptn_mm", F.month("_ingestion_date")) \
                .withColumn("ptn_dd", F.dayofmonth("_ingestion_date"))

        elif self.config_instance.transaction_date_col_name: # partition date column specified
            # create partitions column from specified date column
            self.transformed_df = self.transformed_df \
                .withColumn("_temp_partition_date", F.date_format(
                    F.to_date(
                        self.config_instance.transaction_date_col_name, 
                        self.source_date_format
                    ), 
                    "yyyy-MM-dd"
                )) \
                .withColumn("ptn_yyyy", F.year("_temp_partition_date")) \
                .withColumn("ptn_mm", F.month("_temp_partition_date")) \
                .withColumn("ptn_dd", F.dayofmonth("_temp_partition_date")) \
                .drop("_temp_partition_date")
        else:
            raise Exception("Partition Transformation Exceptional Case. Please, check.")
        
        # 5. re-arrage columns
        target_columns = [column_config.get("target_column") for column_config in self.config_instance.target_schema]
        
        self.transformed_df = self.transformed_df.select(
            "_kde_id",
            *target_columns,
            "_ingestion_date",
            "ptn_yyyy",
            "ptn_mm",
            "ptn_dd",
        )

    def process(self):
        """
        Class Main Process
        """
        self.logger.info("Staring process")
        self.transform()
        self.enrich()
