from abc import ABC, abstractmethod


# Abstract Class
class AbstractIOReaderWriter(ABC):
    """Abstract class for reading/writing data depending on source type and method"""

    @abstractmethod
    def is_db_exists(self):
        pass

    @abstractmethod
    def connect_db(self):
        pass

    @abstractmethod
    def render_sql(self):
        pass

    @abstractmethod
    def init_db(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self):
        pass


class AbstractIOProcessor(ABC):
    """Abstract class as an entrypoint for choosing a type of Reader/Writer, then parse parameters to them"""
    
    @abstractmethod
    def process(self):
        pass


class AbstractMLService(ABC):
    """Abstract class for managing ml-related processes"""

    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def retrain(self):
        pass

    @abstractmethod
    def inference(self):
        pass


class AbstractMLProcessor(ABC):
    """Abstract class as an entrypoint for choosing a type of ML Service, then parse parameters to them"""

    @abstractmethod
    def process(self):
        pass
