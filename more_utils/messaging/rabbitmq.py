import argparse
from .base import (
    AbstractMessengingClient,
    AbstractMessagePublisher,
    AbstractMessageReceiver,
    AbstractMessengingFactory,
)
from pycloudmessenger.rabbitmq import RabbitContext, RabbitClient, RabbitQueue


class RabbitMQPublisher(AbstractMessagePublisher):
    def __init__(self, client):
        self._client = client

    def publish(self, *args, **kwargs):
        self._client.publish(*args, **kwargs)


class RabbitMQReceiver(AbstractMessageReceiver):
    def __init__(self, client):
        self._client = client

    def receive(self, *args, **kwargs):
        return self._client.receive(*args, **kwargs)


class RabbitMQClient(AbstractMessengingClient):
    def __enter__(self):
        return self

    def __init__(self, context, publish=None, subscribe=None):
        self._client = RabbitClient(context)
        self._publisher = RabbitMQPublisher(self._client)
        self._receiver = RabbitMQReceiver(self._client)
        pub_queue_name = publish if publish else context.feeds()
        sub_queue_name = subscribe if subscribe else context.replies()
        self._connect(pub_queue_name, sub_queue_name)

    def _connect(self, pub_queue_name, sub_queue_name):
        self._client.start(
            publish=RabbitQueue(pub_queue_name, durable=True, purge=True),
            subscribe=RabbitQueue(sub_queue_name, durable=True),
        )

    def get_publisher(self) -> RabbitMQPublisher:
        return self._publisher

    def get_receiver(self) -> RabbitMQReceiver:
        return self._receiver

    def stop(self):
        self._client.stop()

    def __exit__(self, *args):
        self.stop()


class RabbitMQContext(RabbitContext):
    def client(self, publish: str = "", subscribe: str = "") -> RabbitMQClient:
        return RabbitMQClient(self, publish, subscribe)


class RabbitMQFactory(AbstractMessengingFactory):
    def __init__(self):
        super().__init__()

    @classmethod
    def create_context(cls, args: argparse.Namespace) -> RabbitMQContext:
        return RabbitMQContext.from_credentials_file(
            args.credentials, args.broker_user, args.broker_password
        )
