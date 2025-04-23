from abc import ABC, abstractmethod


class AbstractInputService(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_config(self):
        pass

    @abstractmethod
    def read_data(self):
        pass

    @abstractmethod
    def process(self):
        pass
    