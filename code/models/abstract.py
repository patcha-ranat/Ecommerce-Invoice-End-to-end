from abc import ABC, abstractmethod


# Abstract Class
class BaseInputReader(ABC):
    """Abstract class for reading input data depending on source"""
    def is_data_exists(self):
        pass
    def connect_db(self):
        pass
    def init_data(self):
        pass
    def read(self):
        pass


class BaseOutputWriter(ABC):
    """Abstract class for writing output data depending on destination"""
    @abstractmethod
    def write(self):
        pass


class BaseMlService(ABC):
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