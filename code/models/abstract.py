from abc import ABC, abstractmethod


# Abstract Class
class AbstractIOReaderWriter(ABC):
    """Abstract class for reading/writing data depending on source type and method"""

    def is_db_exists(self):
        pass

    def connect_db(self):
        pass

    def render_sql(self):
        pass

    def init_db(self):
        pass

    def read(self):
        pass

    def write(self):
        pass


class AbstractIOProcessor:
    """Abstract class as an entrypoint for choosing a type of Reader/Writer then parse parameters to them"""

    def process(self):
        pass


class AbstractMlService(ABC):
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
