# import os
from pathlib import Path
import logging

import pandas as pd
import duckdb

from abstract import BaseInputReader, BaseOutputWriter


class LocalInputReader(BaseInputReader):
    """
    Input Reader to read input from various source depending on choice.

    :param method: *(Required)* Parameter to specify type of source to read from.\n
        Example:\n
            method = "db"\n
        Parameter Choices: ["db", "filesystem"]
    :type method: str

    :param path: *(Required)* Parameter to specify relative path to input data, regardless data model.\n
        Example:\n
            path = "path/to/local/database.db"\n
            path = "path/to/local/file/input.parquet"\n
    :type path: str

    :param data_model: *(Optional)* Parameter to specify data or table to read from database in `schema.table` format. (unused for filesystem)\n
        Example: data_model = "ecomm_gold.sales_transaction"\n
        Parameter Choices: ["ecomm_gold.sales_transaction", "ecomm_gold.customer_rfm"]
    :type data_model: str

    :param init_script_path: *(Optional)* Parameter to customize init script path for 'db' method. default: 'init_script/duckdb.sql'\n
    :type init_script_path: str

    :return: Input data as a DataFrame
    :rtype: pd.DataFrame

    """

    def __init__(
        self,
        method: str,
        path: str,
        data_model: str = None,
        init_script_path: str = "init_script/duckdb.sql",
    ) -> pd.DataFrame:
        super().__init__()
        self.method = method
        self.path = path
        self.data_model = data_model
        self.init_script_path = init_script_path

        if self.is_data_exists():
            pass
        else:
            self.init_data()

        return self.read()

    @staticmethod
    def is_data_exists(path: str) -> bool:
        # os.path.isfile(path=path) -> bool
        data_file = Path(path)
        return data_file.is_file()

    @staticmethod
    def connect_db(path: str):
        con = duckdb.connect(path)
        cursor = con.cursor()
        return cursor

    def init_data(self) -> None:
        if self.method == "db":
            # connect local db
            cursor = self.connect_db(path=self.path)

            # read initial script
            with open(self.init_script_path, "r") as sql_f:
                statement = sql_f.read()

            # execute sql
            cursor.execute(statement)
            # .execute() return connection
            # .sql() return relation (table)

            # close file
            sql_f.close()

        elif self.method == "filesystem":
            logging.info(
                "`method` parameter is given as 'filesystem', no initialization required"
            )
        else:
            raise Exception("Unacceptable `method` argument for Reader.")

    def read(self) -> pd.DataFrame:
        if self.method == "db":
            # connect db
            cursor = self.connect_db(path=self.path)

            # execute reading input statement, then load to pandas dataframe
            df = cursor.sql(f"SELECT * FROM {self.data_model}").to_df()

            return df

        elif self.method == "filesystem":
            # check redundant argument
            if self.data_model:
                logging.warning(
                    "`data_model` argument is not used, reading filesystem path."
                )

            # reading file
            logging.info(f"Reading file: {Path(self.path).name}")
            # Path().stem, Path().suffix, Path().name, Path().parent
            df = pd.read_parquet(self.path)

            return df

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class GCPInputReader(BaseInputReader):
    pass


class DockerDatabaseInputReader(BaseInputReader):
    pass


class InputProcessor:
    """
    Entrypoint for InputReader instance, selecting connection/environment type by given parameter.
    """

    def __init__(
        self,
        source_type: str,
        method: str,
        path: str = None,
        data_model: str = None,
        init_script_path: str = None,
    ):
        self.source_type = source_type
        self.method = method
        self.path = path
        self.data_model = data_model
        self.init_script_path = init_script_path
        self.factory = {
            "local": LocalInputReader,
            "postgresql": DockerDatabaseInputReader,
            "gcp": GCPInputReader,
        }

    def process(self):
        reader_instance = self.factory.get(self.source_type)
        reader_args = {
            "method": self.method,
            "path": self.path,
            "data_model": self.data_model,
            "init_script_path": self.init_script_path,
        }
        input_df = reader_instance(**reader_args)

        if reader_instance:
            return input_df
        else:
            raise Exception("No InputReader assigned in InputProcessor factory")


class LocalOutputWriter(BaseOutputWriter):
    pass


class GCPOutputWriter(BaseOutputWriter):
    pass


class DockerDatabaseOutputWriter(BaseOutputWriter):
    pass


# class OutputProcessor:
#     def __init__(
#         self,
#         target_type: str,
#         method: str,
#         path: str = None,
#         data_model: str = None,
#         init_script_path: str = None,
#     ):
#         self.source_type = target_type
#         self.method = method
#         self.path = path
#         self.data_model = data_model
#         self.init_script_path = init_script_path
#         self.factory = {
#             "local": LocalOutputWriter,
#             "postgresql": DockerDatabaseOutputWriter,
#             "gcp": GCPOutputWriter,
#         }

#     def process(self):
#         writer_instance = self.factory.get(self.source_type)
#         writer_args = {
#             "method": self.method,
#             "path": self.path,
#             "data_model": self.data_model,
#         }
#         input_df = writer_instance(**writer_args)

#         if writer_instance:
#             return input_df
#         else:
#             raise Exception("No OutputWriter assigned in OutputProcessor factory")
