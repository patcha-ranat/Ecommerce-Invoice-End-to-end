from abc import ABC, abstractmethod


class AbstractIngestionProcessor(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def process(self):
        pass
