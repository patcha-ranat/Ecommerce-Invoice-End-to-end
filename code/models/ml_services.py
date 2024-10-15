from abstract import AbstractMLService, AbstractMLProcessor


class BaseMLService(AbstractMLService):
    def __init__(self):
        super().__init__()

    def etc_method():
        pass

class CustomerProfilingProcessor(BaseMLService):
    pass


class CustomerSegmentationProcessor(BaseMLService):
    pass


class MlProcessor(AbstractMLProcessor):
    pass
