from abc import ABC, abstractmethod


class AbstractArgumentService(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def log(self):
        pass

    @abstractmethod
    def process(self):
        pass
