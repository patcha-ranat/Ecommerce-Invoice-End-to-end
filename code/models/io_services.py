import os
from typing import Any
from pathlib import Path
import logging
import json
import warnings

import pandas as pd
import duckdb
import pickle
from google.cloud import storage

from abstract import AbstractInputReader, AbstractOutputWriter, AbstractIOProcessor


# suppress unrelated warnings
warnings.filterwarnings("ignore")

class BaseIOReaderWriter:
    def __init__(self):
        pass

    @property
    def __str__(self):
        bases = [base.__name__ for base in self.__class__.__bases__]
        bases.append(self.__class__.__name__)
        return ".".join(bases)


class BaseInputReader(AbstractInputReader, BaseIOReaderWriter):
    def __init__(self):
        super().__init__()


class BaseOutputWriter(AbstractOutputWriter, BaseIOReaderWriter):
    def __init__(self):
        super().__init__()


class BaseIOProcessor(AbstractIOProcessor):
    def __init__(self):
        super().__init__()

    @property
    def __str__(self):
        bases = [base.__name__ for base in self.__class__.__bases__]
        bases.append(self.__class__.__name__)
        return ".".join(bases)


class LocalInputReader(BaseInputReader):
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
        exec_date: str,
        input_path: str,
        output_path: str,
        init_script_path: str = "db/sql/init_sales.sql",
        init_data_path: str = "../../data/ecomm_invoice_transaction.parquet",
        *args,
        **kwargs,
    ):
        super().__init__()
        self.method = method
        self.exec_date = exec_date #TODO: implement local input path partition 
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        # sql_path
        # data_model
        self.init_script_path = init_script_path
        self.init_data_path = init_data_path

        if method == "db":
            if self.is_db_exists(input_path):
                logging.info("Input database exists, no initialization required")
            else:
                logging.info("Input database not exists, method 'db', initializing db...")
                self.init_db()
        elif method == "filesystem":
            logging.warning(
                "method 'filesystem', no initialization required"
            )
        else:
            raise Exception(
                "Database is not exist. Unacceptable `method` argument for Reader."
            )

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

    def read_data(self, input_path: Path):
        logging.info(f"Reading data file: {input_path.name}")
        # Path().stem, Path().suffix, Path().name, Path().parent
        
        df = pd.read_parquet(input_path)
        
        logging.info(f"Successfully read data file: {input_path.name}")

        return df

    def is_model_exist(
            self, 
            model_path: str | Path,
            model_name: str = None
        ) -> bool:
        """
        Check if interpreter is available

        Return
        ------
        - is_model_exist flag
        - path to model if exists with latest version (None if no model exists)
        """
        
        model_files = self.list_model_in_path(
            model_path=model_path, 
            model_name=model_name
        )

        if len(model_files) != 0:
            return True
        else:
            return False
        
    @staticmethod
    def list_model_in_path(
        model_path: str | Path, 
        model_name: str = None
    ) -> list[str]:
        """
        List models in the specified folder
        """
        files = os.listdir(model_path)
        
        if model_name is not None:
            model_files = [file for file in files if model_name in file]
        else:
            model_files = files
        
        return model_files
    
    def find_latest_model(
        self, 
        model_path: str | Path, 
        model_name: str
    ) -> Path | None:
        """
        Find the latest model version path with specified model name
        """
        model_files = self.list_model_in_path(
            model_path=model_path,
            model_name=model_name
        )

        if len(model_files) != 0:
            # retrieve the latest model version as a name
            latest_model = Path(max(model_files))
            
            return latest_model
        else:
            return None
    
    def read_interpreter(self, output_path: Path) -> Any:
        """
        Read interpreter model with pickle

        Return
        ------
        Model: Any
        """

        model_path = output_path / "models"

        logging.info(f"Searching for Interpreter Model from: {model_path}")

        # create folder if not exist
        model_path.mkdir(parents=True, exist_ok=True)

        if self.is_model_exist(model_path=model_path, model_name="interpreter"):
            logging.info("Interpreter is available, Read Interpreter...")

            # find latest version
            latest_mdoel = self.find_latest_model(
                model_path=model_path,
                model_name="interpreter"
            )
            logging.info(f"Found the latest model as: {latest_mdoel}")

            # read model with pickle
            with open(model_path / latest_mdoel, "rb") as f:
                interpreter = pickle.load(f)
                f.close()
            logging.info("Successfully Read Interpreter")
        else:
            logging.info("Interpreter is not available, Skipped")

            interpreter = None
        
        return interpreter

    def read(self, sql_path: str = None, sql_params: dict = None) -> pd.DataFrame:
        if self.method == "db":
            # logging.info(f"Reading db path from: {self.input_path}")

            # # connect db
            # cursor, con = self.connect_db(path=sql_path)

            # # execute reading input statement, then load to pandas dataframe
            # statement = self.render_sql(sql_path)
            # df = cursor.sql(statement, parameters=sql_params).to_df()

            # con.close()
            # return df
            raise Exception("Method 'db' is not implemented yet.")

        elif self.method == "filesystem":
            logging.info(f"Reading filesystem path from: {self.input_path}")

            # reading data
            df = self.read_data(input_path=self.input_path)

            # interpreter
            interpreter = self.read_interpreter(output_path=self.output_path)

            # formulate input for model services
            inputs: dict = {
                "df": df,
                "interpreter": interpreter
            }

            return inputs

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class GCPInputReader(BaseInputReader):
    def __init__(
        self,
        method: str,
        project_id: str,
        exec_date: str,
        input_path: str,
        output_path: str,
        *args,
        **kwargs,
    ):
        super().__init__()
        self.method = method
        self.project_id = project_id
        self.exec_date = exec_date
        self.input_path = input_path
        self.output_path = output_path

        if method == "db":
            if self.is_db_exists(input_path):
                logging.info("Input database exists")
            else:
                raise Exception("Database or dataset is not exist")
        elif method == "filesystem":
            # validate uri format
            is_input_path_valid, is_output_path_valid = self.is_uri_valid(
                input_path=self.input_path,
                output_path=self.output_path
            )
            if is_input_path_valid and is_output_path_valid:
                logging.info("input_path and output_path are valid.")
            else:
                raise Exception("Either input_path or output_path is invalid format. (gs://) is required.")

        else:
            raise Exception(
                "Database is not exist. Unacceptable `method` argument for Reader."
            )
    
    @staticmethod
    def is_db_exists():
        # TODO: implement db method 
        pass

    @staticmethod
    def connect_db():
        # TODO: implement db method 
        pass

    @staticmethod
    def render_sql(file_path: str) -> str:
        pass

    @staticmethod
    def is_uri_valid(input_path: str, output_path: str) -> bool:
        """Verify URI input"""
        # input_path
        input_parts = input_path.split("/")
        if ("gs:" in input_parts) and (len(input_parts) >= 3):
            is_input_path_valid = True
        else:
            is_input_path_valid = False
        
        # output
        output_parts = output_path.split("/")
        if ("gs:" in output_parts) and (len(output_parts) >= 3):
            is_output_path_valid = True
        else:
            is_output_path_valid = False

        return is_input_path_valid, is_output_path_valid

    def read_data(self, input_path: str):
        """
        Read Data from specified path
        
        :input_path params: 'gs://{bucket}/{prefix}'
        :input_path Path:
        """
        file_name = input_path.split("/")[-1]
        logging.info(f"Reading data file: {file_name}")
        # Path().stem, Path().suffix, Path().name, Path().parent, Path().parts
        
        df = pd.read_parquet(input_path)
        
        logging.info(f"Successfully read data file: {file_name}")

        return df

    def list_model_in_path(self, model_path: str, model_name: str = None) -> list[str]:
        """
        List models in the specified folder
        """
        # get components
        bucket_name = model_path.split("/")[2]
        prefix_name = "/".join(model_path.split("/")[3:])

        storage_client = storage.Client(project=self.project_id)

        blobs = storage_client.list_blobs(
            bucket_or_name=bucket_name,
            prefix=prefix_name
        )

        blobs_list = list(blobs)
        
        if model_name is not None:
            model_files = [file.name for file in blobs_list if model_name in file.name]
        else:
            model_files = blobs_list
        
        return model_files

    def is_model_exist(self, model_path: str, model_name: str = None) -> bool:
        """
        Check if interpreter is available

        Return
        ------
        - is_model_exist flag
        - path to model if exists with latest version (None if no model exists)
        """
        
        model_files = self.list_model_in_path(
            model_path=model_path, 
            model_name=model_name
        )

        if len(model_files) != 0:
            return True
        else:
            return False
    
    def find_latest_model(
        self, 
        model_path: str, 
        model_name: str
    ) -> str | None:
        """
        Find the latest model version path with specified model name
        """
        model_files = self.list_model_in_path(
            model_path=model_path,
            model_name=model_name
        )

        if len(model_files) != 0:
            # retrieve the latest model version as a name
            latest_model = max(model_files)
            
            return latest_model
        else:
            return None
    
    def read_model_blob(self, path: str):
        bucket_name = path.split("/")[2]
        target_path = "/".join(path.split("/")[3:])

        storage_client = storage.Client(project=self.project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(target_path)

        with blob.open("rb") as f:
            model = pickle.load(f)
            f.close()
        
        return model

    def read_interpreter(self, output_path: str) -> Any:
        """
        Read interpreter model with pickle

        Return
        ------
        Model: Any
        """

        logging.info(f"Searching for Interpreter Model from: {output_path}")

        model_path = f"{output_path}/models"

        if self.is_model_exist(model_path=model_path, model_name="interpreter"):
            logging.info("Interpreter is available, Read Interpreter...")

            # find latest version
            latest_mdoel = self.find_latest_model(
                model_path=model_path,
                model_name="interpreter"
            )
            logging.info(f"Found the latest model as: {latest_mdoel}")

            # re-structure uri 
            bucket_name = model_path.split("/")[2]
            model_path = f"gs://{bucket_name}/{latest_mdoel}"

            # read model with pickle
            interpreter = self.read_model_blob(path=model_path)

            logging.info("Successfully Read Interpreter")
        else:
            logging.info("Interpreter is not available, Skipped")

            interpreter = None
        
        return interpreter

    def read(self, sql_path: str = None, sql_params: dict = None) -> pd.DataFrame:
        if self.method == "db":
            # logging.info(f"Reading db path from: {self.input_path}")

            # # connect db
            # cursor, con = self.connect_db(path=sql_path)

            # # execute reading input statement, then load to pandas dataframe
            # statement = self.render_sql(sql_path)
            # df = cursor.sql(statement, parameters=sql_params).to_df()

            # con.close()
            # return df
            raise Exception("Method 'db' is not implemented yet.")

        elif self.method == "filesystem":
            logging.info(f"Reading filesystem path from: {self.input_path}")

            # reading data
            df = self.read_data(input_path=self.input_path)

            # interpreter
            interpreter = self.read_interpreter(output_path=self.output_path)

            # formulate input for model services
            inputs: dict = {
                "df": df,
                "interpreter": interpreter
            }

            return inputs

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class APIInputReader(BaseInputReader):
    pass


class InputProcessor(BaseIOProcessor):
    """
    Entrypoint for InputReader instance, selecting connection/environment type by given parameters.
    """

    def __init__(
        self,
        env: str,
        method: str,
        exec_date: str,
        input_path: str = None,
        output_path: str = None,
        project_id: str = None
    ):
        self.env = env
        self.method = method
        self.project_id = project_id
        self.exec_date = exec_date
        self.input_path = input_path
        self.output_path = output_path
        self.factory = {
            "local": LocalInputReader,
            "postgresql": APIInputReader,
            "gcp": GCPInputReader,
        }

    def process(self):
        logging.info(f"Input Processor: {self.__str__}")

        reader_instance = self.factory.get(self.env)
        reader_args = {
            "method": self.method,
            "project_id": self.project_id,
            "exec_date": self.exec_date,
            "input_path": self.input_path,
            "output_path": self.output_path
        }
        reader = reader_instance(**reader_args)
        
        logging.info(f"Selected reader: {reader.__str__}")
        logging.info(f"Reader arguments: {reader_args}")

        if reader_instance:
            return reader.read()
        else:
            raise Exception("No InputReader assigned in InputProcessor factory")


class LocalOutputWriter(BaseOutputWriter):
    def __init__(
        self,
        method: str,
        output_path: str,
        exec_date: str,
        output: dict,
        *args,
        **kwargs
    ) -> None:
        super().__init__()
        self.method = method
        self.output_path = Path(output_path)
        self.exec_date = exec_date
        self.output = output

    def build_control_file_dict(self, artifacts: dict[str, Any]) -> dict:
        control_file_dict: dict = {}
        control_file_dict["interpreter_params"] = artifacts.get("interpreter_params")
        control_file_dict["interpreter_metrics"] = artifacts.get("interpreter_metrics")
        control_file_dict["is_train_interpreter"] = artifacts.get("is_train_interpreter")
        control_file_dict["is_anomaly_exist"] = artifacts.get("is_anomaly_exist")

        return control_file_dict

    def find_latest_model_version(
        self,
        model_path: str | Path,
        model_name: str
    ) -> int:
        latest_model = self.find_latest_model(
            model_path=model_path, 
            model_name=model_name
        )
        latest_version = Path(latest_model).name.split("_v")[-1]

        return latest_version

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
            # date = datetime.today().date().strftime("%Y-%m-%d")
            data_path = self.output_path / "data" / self.exec_date

            # create directory if not exist
            data_path.mkdir(parents=True, exist_ok=True)

            # export
            output.to_parquet(data_path / data_file_name)
            
            # logs
            logging.info(f"Successfully export data to {str(data_path / data_file_name)}")
        
        elif element_type == "model":
            # prep path
            model_path = self.output_path / "models"

            # create directory if not exist
            model_path.mkdir(parents=True, exist_ok=True)
            
            # dynamic bump model version if available
            files = os.listdir(model_path)
            model_files = [file for file in files if file.startswith(filename)]

            if len(model_files) != 0:
                version = int(Path(max(model_files)).stem.split("_v")[1]) + 1
            else:
                version = 1
            
            # prep filename and path
            model_file_name = f"{filename}_v{version}.pkl"
            model_path = model_path / model_file_name

            # export
            with open(model_path, "+wb") as f:
                pickle.dump(output, f)
                f.close()

            # logs
            logging.info(f"Successfully export model to {str(model_path / model_file_name)}")

        elif element_type == "artifact":
            # aka control file as json
            # prep filename and path
            artifact_file_name = f"{filename}.json"
            artifact_path = self.output_path / "artifact" / self.exec_date

            # create directory if not exist
            artifact_path.mkdir(parents=True, exist_ok=True)

            # export
            with open(artifact_path / artifact_file_name, "+w") as f:
                json.dump(output, f, indent=4)
                f.close()
            
            # anomaly cluster flag (revert)
            # if output.get("is_anomaly_exist"):
            #     with open(artifact_path / "ANOMALY_EXIST", "w") as f:
            #         f.close()
            #     logging.info(f"Successfully export anomaly flag file (artifact) to {artifact_path / 'ANOMALY_EXIST'}")

            # logs
            logging.info(f"Successfully export control file (artifact) to {str(artifact_path / artifact_file_name)}")

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
            logging.info(f"Writing file to: {Path(self.output_path)}")
            
            # data model
            logging.info("Exporting... Data Models")
            df_cluster_rfm: pd.DataFrame = self.output.get("df_cluster_rfm")
            df_cluster_importance: pd.DataFrame = self.output.get("df_cluster_importance")

            self.write_element(output=df_cluster_rfm, element_type="data", filename="df_cluster_rfm")
            self.write_element(output=df_cluster_importance, element_type="data", filename="df_cluster_importance")

            # ml model
            logging.info("Exporting... ML Models")
            segmenter_trained = self.output.get("segmenter_trained")
            segmenter_scaler = self.output.get("segmenter_scaler")
            interpreter = self.output.get("interpreter")

            self.write_element(output=segmenter_trained, element_type="model", filename="kmeans_segmenter")
            self.write_element(output=segmenter_scaler, element_type="model", filename="segmenter_scaler")
            self.write_element(output=interpreter, element_type="model", filename="lgbm_interpreter")

            # other artifacts
            logging.info("Exporting... Artifact (Control file)")

            # aggregate artifact
            artifacts = {
                "interpreter_params": self.output.get("interpreter_params"),
                "interpreter_metrics": self.output.get("interpreter_metrics"),
                "is_train_interpreter": self.output.get("is_train_interpreter"), # boolean,
                # "latest_trained_model_version": self.find_latest_model_version(model_path=), #TODO: solve this logic finding the latest trained model version
                "is_anomaly_exist": self.output.get("is_anomaly_exist") # boolean
            }
            control_file_dict =  self.build_control_file_dict(artifacts=artifacts)

            self.write_element(output=control_file_dict, element_type="artifact", filename="control_file")

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class GCPOutputWriter(BaseOutputWriter):
    def __init__(
        self,
        method: str,
        project_id: str,
        output_path: str,
        exec_date: str,
        output: dict,
    ) -> None:
        super().__init__()
        self.method = method
        self.project_id = project_id
        self.output_path = output_path
        self.exec_date = exec_date
        self.output = output

    def build_control_file_dict(self, artifacts: dict[str, Any]) -> dict:
        control_file_dict: dict = {}
        control_file_dict["interpreter_params"] = artifacts.get("interpreter_params")
        control_file_dict["interpreter_metrics"] = artifacts.get("interpreter_metrics")
        control_file_dict["is_train_interpreter"] = artifacts.get("is_train_interpreter")
        control_file_dict["is_anomaly_exist"] = artifacts.get("is_anomaly_exist")

        return control_file_dict

    def list_model_in_path(self, model_path: str, model_name: str = None) -> list[str]:
        """
        List models in the specified folder
        """
        # get components
        bucket_name = model_path.split("/")[2]
        prefix_name = "/".join(model_path.split("/")[3:])

        storage_client = storage.Client(project=self.project_id)

        blobs = storage_client.list_blobs(
            bucket_or_name=bucket_name,
            prefix=prefix_name
        )

        blobs_list = list(blobs)
        
        if model_name is not None:
            model_files = [file.name for file in blobs_list if model_name in file.name]
        else:
            model_files = blobs_list
        
        return model_files

    def find_latest_model(
        self, 
        model_path: str, 
        model_name: str
    ) -> Path | None:
        """
        Find the latest model version path with specified model name
        """
        model_files = self.list_model_in_path(
            model_path=model_path,
            model_name=model_name
        )

        if len(model_files) != 0:
            # retrieve the latest model version as a name
            latest_model = max(model_files)
            
            return latest_model
        else:
            return None

    def find_latest_model_version(
        self,
        model_path: str,
        model_name: str
    ) -> int:
        latest_model: str = self.find_latest_model(
            model_path=model_path, 
            model_name=model_name
        )
        latest_version = latest_model.split("/")[-1].split("_v")[-1].split(".")[0]

        return int(latest_version)

    def is_model_exist(self, model_path: str, model_name: str = None) -> bool:
        """
        Check if interpreter is available

        Return
        ------
        - is_model_exist flag
        - path to model if exists with latest version (None if no model exists)
        """
        
        model_files = self.list_model_in_path(
            model_path=model_path, 
            model_name=model_name
        )

        if len(model_files) != 0:
            return True
        else:
            return False
        
    def write_blob(self, path: str, output: Any, how: str) -> None:
        """
        Upload output element (model/json file) to GCS

        :param path: exact path to target
        :type path: Path
        
        :param model: output element
        :type model: Any
        
        :param how: ['pickle', 'json']
        :type how: str
        """
        bucket_name = path.split("/")[2]
        target_path = "/".join(path.split("/")[3:])

        storage_client = storage.Client(project=self.project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(target_path)

        if how == "pickle": # binary
            with blob.open("wb") as f:
                pickle.dump(output, f)
                f.close()
        elif how == "json":
            with blob.open("w") as f:
                json.dump(output, f, indent=4)
                f.close()
        elif how == "none":
            blob.upload_from_string("")
        else:
            raise Exception("'how' parameter is not valid.")

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
            # date = datetime.today().date().strftime("%Y-%m-%d")
            data_path = f"{self.output_path}/data/{self.exec_date}/{data_file_name}"

            # export
            output.to_parquet(data_path)
            
            # logs
            logging.info(f"Successfully export data to {data_path}")
        
        elif element_type == "model":
            # prep path
            model_path = f"{self.output_path}/models"
            
            # dynamic bump model version if available
            if self.is_model_exist(model_path=model_path, model_name="interpreter"):
                latest_version = self.find_latest_model_version(model_path=model_path, model_name=filename)
                version = latest_version + 1
            else:
                version = 1
            
            # prep filename and path
            model_file_name = f"{filename}_v{version}.pkl"
            model_path = f"{model_path}/{model_file_name}"

            # export
            self.write_blob(path=model_path, output=output, how="pickle")

            # logs
            logging.info(f"Successfully export model to {model_path}")

        elif element_type == "artifact":
            # aka control file as json
            # prep filename and path
            artifact_file_name = f"{filename}.json"
            # artifact_flag_path = f"{self.output_path}/artifact/{self.exec_date}/ANOMALY_EXIST" 
            artifact_path = f"{self.output_path}/artifact/{self.exec_date}/{artifact_file_name}"


            # export
            self.write_blob(path=artifact_path, output=output, how="json")

            # anomaly cluster flag
            # if output.get("is_anomaly_exist"):
            #     self.write_blob(path=artifact_flag_path, output=None, how="none")

            #     logging.info(f"Successfully export anomaly flag file (artifact) to {artifact_flag_path}")


            # logs
            logging.info(f"Successfully export control file (artifact) to {artifact_path}")

    def write(self, sql_path: str = None, sql_params: dict = None) -> None:
        if self.method == "db":
            # TODO: implement db method
            # # connect db
            # cursor, con = self.connect_db(path=self.output_path)

            # # reading sql file rendering sql statement
            # statement = self.render_sql(file_path=sql_path)

            # # execute writing output statement to load pandas DataFrame to duckdb database
            # # https://duckdb.org/docs/guides/python/import_pandas.html
            # in_memory_df = self.df
            # in_memory_df_param = "in_memory_df"
            # sql_params = {**sql_params, in_memory_df_param: in_memory_df}
            # cursor.sql(statement, parameters=sql_params)

            # con.close()
            # logging.info(
            #     f"Successfully write output file: {Path(self.output_path).name}"
            # )
            # return 1
            pass

        elif self.method == "filesystem":
            # writing file
            logging.info(f"Writing file to: {Path(self.output_path)}")
            
            # data model
            logging.info("Exporting... Data Models")
            df_cluster_rfm: pd.DataFrame = self.output.get("df_cluster_rfm")
            df_cluster_importance: pd.DataFrame = self.output.get("df_cluster_importance")

            self.write_element(output=df_cluster_rfm, element_type="data", filename="df_cluster_rfm")
            self.write_element(output=df_cluster_importance, element_type="data", filename="df_cluster_importance")

            # ml model
            logging.info("Exporting... ML Models")
            segmenter_trained = self.output.get("segmenter_trained")
            segmenter_scaler = self.output.get("segmenter_scaler")
            interpreter = self.output.get("interpreter")

            self.write_element(output=segmenter_trained, element_type="model", filename="kmeans_segmenter")
            self.write_element(output=segmenter_scaler, element_type="model", filename="segmenter_scaler")
            self.write_element(output=interpreter, element_type="model", filename="lgbm_interpreter")

            # other artifacts
            logging.info("Exporting... Artifact (Control file)")

            # aggregate artifact
            artifacts = {
                "interpreter_params": self.output.get("interpreter_params"),
                "interpreter_metrics": self.output.get("interpreter_metrics"),
                "is_train_interpreter": self.output.get("is_train_interpreter"), # boolean,
                # "latest_trained_model_version": self.find_latest_model_version(model_path=), #TODO: solve this logic finding the latest trained model version
                "is_anomaly_exist": self.output.get("is_anomaly_exist") # boolean
            }
            control_file_dict =  self.build_control_file_dict(artifacts=artifacts)

            self.write_element(output=control_file_dict, element_type="artifact", filename="control_file")

        else:
            raise Exception("Unacceptable `method` argument for Reader.")


class APIOutputWriter(BaseOutputWriter):
    pass


class OutputProcessor(BaseIOProcessor):
    def __init__(
        self, 
        env: str, 
        method: str,
        output_path: str,
        exec_date: str,
        outputs: dict,
        project_id: str = None
    ):
        self.env = env
        self.method = method
        self.project_id = project_id
        self.output_path = output_path
        self.exec_date = exec_date
        self.outputs = outputs
        self.factory = {
            "local": LocalOutputWriter,
            "postgresql": APIOutputWriter,
            "gcp": GCPOutputWriter,
        }

    def log_writer_args(self, writer_args: dict[str, Any]) -> None:
        """
        Exclude DataFrame element from output dict for cleaned logging
        """
        output_dict: dict = writer_args.get("output")
        
        output_dict_output: dict = {}
        output_dict_output["df_cluster_rfm"] = True if output_dict.get("df_cluster_rfm") is not None else False
        output_dict_output["df_cluster_importance"] = True if output_dict.get("df_cluster_importance") is not None else False
        output_dict_output["segmenter_trained"] = True if output_dict.get("segmenter_trained") is not None else False
        output_dict_output["segmenter_scaler"] = True if output_dict.get("segmenter_scaler") is not None else False
        output_dict_output["interpreter"] = True if output_dict.get("interpreter") is not None else False
        output_dict_output["interpreter_params"] = output_dict.get("interpreter_params")
        output_dict_output["interpreter_metrics"] = output_dict.get("interpreter_metrics")
        output_dict_output["is_train_interpreter"] = output_dict.get("is_train_interpreter")
        output_dict_output["is_anomaly_exist"] = output_dict.get("is_anomaly_exist")
        
        output_log = {**writer_args}
        output_log["output"] = output_dict_output

        # pretty logs
        logging.info(f"Writer arguments: {output_log}")

    def process(self) -> None:
        logging.info(f"Output Processor: {self.__str__}")

        writer_instance = self.factory.get(self.env)
        writer_args = {
            "method": self.method,
            "project_id": self.project_id,
            "output_path": self.output_path,
            "exec_date": self.exec_date,
            "output": self.outputs
        }
        writer = writer_instance(**writer_args)

        logging.info(f"Selected writer: {writer.__str__}")
        self.log_writer_args(writer_args)

        if writer_instance:
            return writer.write()
        else:
            raise Exception("No OutputWriter assigned in OutputProcessor factory")
