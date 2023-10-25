import argparse
from typing import Union
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


class RabbitMQConsumer(AbstractMessageReceiver):
    def __init__(self, client):
        self._client = client

    def receive(self, *args, **kwargs):
        return self._client.receive(*args, **kwargs)


class RabbitMQClient(AbstractMessengingClient):
    def __enter__(self):
        return self

    def __init__(self, context, publish=None, subscribe=None):
        self._client = RabbitClient(context)
        pub_queue_name = publish if publish else context.replies()
        sub_queue_name = subscribe if subscribe else context.feeds()
        self._connect(pub_queue_name, sub_queue_name)

    def _connect(self, pub_queue_name, sub_queue_name):
        self._client.start(
            publish=RabbitQueue(pub_queue_name, durable=True),
            subscribe=RabbitQueue(sub_queue_name, durable=True),
        )

    def get_publisher(self) -> RabbitMQPublisher:
        return RabbitMQPublisher(self._client)

    def get_consumer(self) -> RabbitMQConsumer:
        return RabbitMQConsumer(self._client)

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
    def create_context(
        cls,
        args: Union[dict, argparse.Namespace],
        broker_user=None,
        broker_password=None,
    ) -> RabbitMQContext:
        if isinstance(args, dict):
            return RabbitMQContext.from_args(args, broker_user, broker_password)
        else:
            return RabbitMQContext.from_credentials_file(
                args.credentials, broker_user, broker_password
            )
