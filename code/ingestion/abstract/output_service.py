from abc import ABC, abstractmethod


class AbstractOutputService(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_config(self):
        pass
    
    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_partition(self):
        pass

    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def process(self):
        pass
    