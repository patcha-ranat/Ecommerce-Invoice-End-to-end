import os
from typing import Any
from pathlib import Path
import logging
import json

import pandas as pd
import duckdb
import pickle

from abstract import AbstractIOReaderWriter, AbstractIOProcessor


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


class InputProcessor(AbstractIOProcessor):
    """
    Entrypoint for InputReader instance, selecting connection/environment type by given parameters.
    """

    def __init__(
        self,
        env: str,
        method: str,
        input_path: str = None,
    ):
        self.env = env
        self.method = method
        self.input_path = input_path
        self.factory = {
            "local": LocalInputReader,
            "postgresql": DockerDatabaseInputReader,
            "gcp": GCPInputReader,
        }

    def process(self):
        reader_instance = self.factory.get(self.env)
        reader_args = {
            "method": self.method,
            "input_path": self.input_path,
        }
        reader = reader_instance(**reader_args)

        if reader_instance:
            return reader.read()
        else:
            raise Exception("No InputReader assigned in InputProcessor factory")


class LocalOutputWriter(BaseIOReaderWriter):
    def __init__(
        self,
        method: str,
        output_path: str,
        output: dict,
    ) -> None:
        super().__init__()
        self.method = method
        self.output_path = Path(output_path)
        self.output = output

        return self.write()

    def write_element(
        self, 
        output: Any, 
        element_type: str, 
        filename: str
    ) -> None:
        """
        Writing output file depends on arguments

        Parameters
        ----------
        - output: pd.DataFrame | KMeans | LGBMClassifier | dict
        - element_type: str
        - filename: str
        """
        if element_type == "data":
            # prep filename and path
            data_file_name = f"{str(filename)}.parquet"
            data_path = self.output_path + "data" + data_file_name

            # export
            output.to_parquet(data_path)
            
            # logs
            logging.info(f"Successfully export data to {str(data_path)} output as {data_file_name}")
        
        elif element_type == "model":
            # prep path
            model_path = self.output_path + "model"
            
            # dynamic bump model version if available
            files = os.listdir(model_path)
            model_files = [file for file in files if file.startswith(filename)]

            if len(model_files) != 0:
                version = int(Path(max(model_files)).stem.split("_v")[1]) + 1
            else:
                version = 1
            
            # prep filename and path
            model_file_name = f"{filename}_{version}.pkl"
            model_path = model_path + model_file_name

            # export
            with open(model_path, "wb") as f:
                pickle.dump(output, f)
                f.close()

            # logs
            logging.info(f"Successfully export model to {str(model_path)} output as {model_file_name}")

        elif element_type == "artifact":
            # aka control file as json
            # prep filename and path
            artifact_file_name = f"{filename}.json"
            artifact_path = self.output_path + "artifact" + artifact_file_name

            # export
            # TODO: improve exporting control file
            # update control file
            if os.path.isfile(path=artifact_path):
                pass
            # create control file
            else:
                with open(artifact_path, 'w') as f:
                    json.dump(output, f)
                    f.close()

            # logs
            logging.info(f"Successfully export control file (artifact) to {str(artifact_path)} output as {artifact_file_name}")

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
            
            # data model
            df_cluster_rfm: pd.DataFrame = self.output.get("df_cluster_rfm")
            df_cluster_importance: pd.DataFrame = self.output.get("df_cluster_importance")

            self.write_element(output=df_cluster_rfm, element_type="data", filename="df_cluster_rfm")
            self.write_element(output=df_cluster_importance, element_type="data", filename="df_cluster_importance")

            # ml model
            segmenter_trained = self.output.get("segmenter_trained")
            segmenter_scaler = self.output.get("segmenter_scaler")
            interpreter = self.output.get("interpreter")

            self.write_element(output=segmenter_trained, element_type="model", filename="kmeans_segmenter")
            self.write_element(output=segmenter_scaler, element_type="model", filename="segmenter_scaler")
            self.write_element(output=interpreter, element_type="model", filename="lgbm_interpreter")


            # other artifacts
            interpreter_params = self.output.get("interpreter_params")
            interpreter_metrics = self.output.get("interpreter_metrics")
            # is_anomaly_exist = self.output.get("is_anomaly_exist") # boolean
            self.write_element(output=interpreter_params, element_type="artifact", filename="control_file")
            self.write_element(output=interpreter_metrics, element_type="artifact", filename="control_file")
            # self.write_element(output=is_anomaly_exist, element_type="artifact", filename="control_file")

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class GCPOutputWriter(BaseIOReaderWriter):
    pass


class DockerDatabaseOutputWriter(BaseIOReaderWriter):
    pass


class OutputProcessor(AbstractIOProcessor):
    def __init__(
        self, 
        env: str, 
        method: str, 
        output_path: str,
        output: dict
    ):
        self.env = env
        self.method = method
        self.output_path = output_path
        self.output = output
        self.factory = {
            "local": LocalOutputWriter,
            "postgresql": DockerDatabaseOutputWriter,
            "gcp": GCPOutputWriter,
        }

    def process(self):
        writer_instance = self.factory.get(self.env)
        writer_args = {
            "method": self.method,
            "output_path": self.output_path,
            "output": self.output
        }
        writer = writer_instance(**writer_args)

        if writer_instance:
            return writer.write()
        else:
            raise Exception("No OutputWriter assigned in OutputProcessor factory")
