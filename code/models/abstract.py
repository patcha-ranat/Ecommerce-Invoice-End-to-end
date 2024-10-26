from abc import ABC, abstractmethod


# Abstract Class
class AbstractInputReader(ABC):
    """Abstract class for reading data depending on given source type and method"""

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


class AbstractOutputWriter(ABC):
    """Abstract class for writing data depending on given source type and method"""
    
    @abstractmethod
    def write_element(self):
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
    def process(self):
        """Orchestrate all the processes within a service"""
        pass


class AbstractMLProcessor(ABC):
    """Abstract class as an entrypoint to orchestrateall services, then parse parameters to them"""

    @abstractmethod
    def process(self):
        """Orchestrate all the processes between ML services"""
        pass
