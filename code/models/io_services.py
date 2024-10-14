# import os
from pathlib import Path
import logging

import pandas as pd
import duckdb

from abstract import AbstractIOReaderWriter


class BaseIOReaderWriter(AbstractIOReaderWriter):
    def __init__(self):
        super().__init__()

    @staticmethod
    def is_db_exists(input_path: str) -> bool:
        # os.path.isfile(path=input_path) -> bool
        data_file = Path(input_path)
        return data_file.is_file()

    @staticmethod
    def connect_db(input_path: str) -> tuple:
        con = duckdb.connect(input_path)
        cursor = con.cursor()
        return (cursor, con)

    @staticmethod
    def render_sql(file_path: str) -> str:
        with open(file_path, "r") as sql_f:
            statement = sql_f.read()
        sql_f.close()
        return statement


class LocalInputReader(BaseIOReaderWriter):
    """
    :param method: *(Required)* Source type to read from.\n
        Parameters choices: ['db', 'filesystem']
    :type method: str

    :param input_path: *(Required)* Path to a source file or a local db file
    :type input_path: str

    :param init_script_path: *(Optional)* Path to initial script for database initialization
    :type init_script_path: str

    :param init_data_path: *(Optional)* Path to initial data path for database initialization
    :type init_data_path: str
    """

    def __init__(
        self,
        method: str,
        input_path: str,
        init_script_path: str = "db/sql/init_sales.sql",
        init_data_path: str = "../../data/ecomm_invoice_transaction.parquet",
    ):
        super().__init__()
        self.method = method
        self.input_path = input_path
        # sql_path
        # data_model
        self.init_script_path = init_script_path
        self.init_data_path = init_data_path

        if self.is_db_exists(input_path):
            pass
        elif method == "db":
            self.init_db()
        elif method == "filesystem":
            logging.warning(
                "`method` parameter is given as 'filesystem', no initialization required"
            )
        else:
            raise Exception(
                "Database is not exist. Unacceptable `method` argument for Reader."
            )
        return self

    def init_db(self) -> None:
        # connect local db
        cursor, con = self.connect_db(path=self.input_path)

        # read initial script
        statement = self.render_sql(self.init_script_path)
        # execute sql
        sql_params = {
            "data_model": "ecomm_gold.sales_transaction",  # only sales data is acceptable for initialization
            "data_path": self.init_data_path,
        }
        cursor.execute(statement, parameters=sql_params)
        # .execute() return connection
        # .sql() return relation (table)
        con.close()

    def read(self, sql_path: str = None, sql_params: dict = None) -> pd.DataFrame:
        if self.method == "db":
            # connect db
            cursor, con = self.connect_db(path=sql_path)

            # execute reading input statement, then load to pandas dataframe
            statement = self.render_sql(sql_path)
            df = cursor.sql(statement, parameters=sql_params).to_df()

            con.close()
            return df

        elif self.method == "filesystem":
            # check redundant argument
            logging.warning("Reading filesystem path with `input_path` parameter.")

            # reading file
            logging.info(f"Reading file: {Path(self.input_path).name}")
            # Path().stem, Path().suffix, Path().name, Path().parent
            df = pd.read_parquet(self.input_path)
            logging.info(f"Successfully read file: {Path(self.input_path).name}")

            return df

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class GCPInputReader(BaseIOReaderWriter):
    pass


class DockerDatabaseInputReader(BaseIOReaderWriter):
    pass


class InputProcessor:
    """
    Entrypoint for InputReader instance, selecting connection/environment type by given parameters.
    """

    def __init__(
        self,
        source_type: str,
        method: str,
        input_path: str = None,
    ):
        self.source_type = source_type
        self.method = method
        self.input_path = input_path
        self.factory = {
            "local": LocalInputReader,
            "postgresql": DockerDatabaseInputReader,
            "gcp": GCPInputReader,
        }

    def process(self):
        reader_instance = self.factory.get(self.source_type)
        reader_args = {
            "method": self.method,
            "input_path": self.input_path,
        }
        reader = reader_instance(**reader_args)

        if reader_instance:
            return reader
        else:
            raise Exception("No InputReader assigned in InputProcessor factory")


class LocalOutputWriter(BaseIOReaderWriter):
    def __init__(
        self,
        df: pd.DataFrame,
        method: str,
        output_path: str,
    ) -> None:
        super().__init__()
        self.df = df
        self.method = method
        self.output_path = output_path

        return self.write()

    def write(self, sql_path: str = None, sql_params: dict = None) -> None:
        if self.method == "db":
            # connect db
            cursor, con = self.connect_db(path=self.output_path)

            # reading sql file rendering sql statement
            statement = self.render_sql(file_path=sql_path)

            # execute writing output statement to load pandas DataFrame to duckdb database
            # https://duckdb.org/docs/guides/python/import_pandas.html
            in_memory_df = self.df
            in_memory_df_param = "in_memory_df"
            sql_params = {**sql_params, in_memory_df_param: in_memory_df}
            cursor.sql(statement, parameters=sql_params)

            con.close()
            logging.info(
                f"Successfully write output file: {Path(self.output_path).name}"
            )
            return 1

        elif self.method == "filesystem":

            # writing file
            logging.info(f"Writing file: {Path(self.output_path).name}")
            self.df.to_parquet(self.output_path)

            return 1

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class GCPOutputWriter(BaseIOReaderWriter):
    pass


class DockerDatabaseOutputWriter(BaseIOReaderWriter):
    pass


class OutputProcessor:
    def __init__(
        self, target_type: str, method: str, output_path: str, df: pd.DataFrame
    ):
        self.target_type = target_type
        self.method = method
        self.output_path = output_path
        self.df = df
        self.factory = {
            "local": LocalOutputWriter,
            "postgresql": DockerDatabaseOutputWriter,
            "gcp": GCPOutputWriter,
        }

    def process(self):
        writer_instance = self.factory.get(self.target_type)
        writer_args = {
            "df": self.df,
            "method": self.method,
            "output_path": self.output_path,
        }
        writer = writer_instance(**writer_args)

        if writer_instance:
            return writer
        else:
            raise Exception("No OutputWriter assigned in OutputProcessor factory")
