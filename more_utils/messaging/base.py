import abc


class AbstractMessengingClient(abc.ABC):
    """
    abstract messenger client
    """

    @abc.abstractmethod
    def __init__(self):
        super().__init__()

    def connect(self, publish, subscribe):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_publisher(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_receiver(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def stop(self):
        raise NotImplementedError()


class AbstractMessagePublisher(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def publish(self, **kwargs):
        raise NotImplementedError()


class AbstractMessageReceiver(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        super().__init__()

    @abc.abstractmethod
    def receive(self, **kwargs):
        raise NotImplementedError()


class AbstractMessengingFactory(abc.ABC):
    @abc.abstractmethod
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    @abc.abstractmethod
    def create_context(cls, args):
        raise NotImplementedError()
