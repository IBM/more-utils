import abc


class AbstractDBLayer(abc.ABC):
    """
    abstract database client layer
    """

    @abc.abstractmethod
    def __init__(self):
        pass

    @classmethod
    @abc.abstractmethod
    def connect(cls):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_session(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError()


class AbstractDBSession(abc.ABC):
    """
    abstract database session
    """

    @abc.abstractmethod
    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def execute(self, query):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def insert(self, data):
        raise NotImplementedError()

    @abc.abstractmethod
    def close(self):
        raise NotImplementedError()
