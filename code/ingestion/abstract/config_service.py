from abc import ABC, abstractmethod

class AbstractConfigService(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def generate_schema(self):
        pass

    @abstractmethod
    def generate_patition_attribute(self):
        pass

    @abstractmethod
    def generate(self):
        pass

    @abstractmethod
    def log(self):
        pass

    @abstractmethod
    def process(self):
        pass
