import io
from uuid import uuid4

from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import (
    Deserializer,
    MessageField,
    SerializationContext,
    SerializationError,
    Serializer,
    StringSerializer,
)
from google.protobuf.message_factory import MessageFactory

from guardium_utils.logging import _setup_logging

logger = _setup_logging("Guardium-Utils", "INFO")


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    logger.info(
        "Message record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


class _ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class DefaultProtobufSerializer(Serializer):
    def __init__(self, message_type) -> None:
        descriptor = message_type.DESCRIPTOR
        self._msg_class = MessageFactory().GetPrototype(descriptor)

    def __call__(self, message, ctx: SerializationContext):
        if message is None:
            return None

        if not isinstance(message, self._msg_class):
            raise ValueError(
                "message must be of type {} not {}".format(
                    self._msg_class, type(message)
                )
            )

        with _ContextStringIO() as fo:
            fo.write(message.SerializeToString())
            return fo.getvalue()


class DefaultProtobufDeserializer(Deserializer):
    def __init__(self, message_type) -> None:
        descriptor = message_type.DESCRIPTOR
        self._msg_class = MessageFactory().GetPrototype(descriptor)

    def __call__(self, data, ctx: SerializationContext):
        with _ContextStringIO(data) as payload:
            try:
                message = self._msg_class()
                message.ParseFromString(payload.read())
                return message

            except Exception as e:
                raise SerializationError(
                    f"Failed to decode payload with message type: {self._msg_class}"
                    + str(e)
                )


class KafkaProducer:
    def __init__(
        self, host, port, stream_key, serializer=None, **stream_configs
    ) -> None:
        self.stream_key = stream_key
        self.serializer = serializer
        self.producer = Producer({"bootstrap.servers": host + ":" + str(port)})

    def produce(self, data, partition=0):
        try:
            self.producer.produce(
                topic=self.stream_key,
                partition=partition,
                key=StringSerializer("utf8")(str(uuid4())),
                value=self.serializer(
                    data,
                    SerializationContext(self.stream_key, MessageField.VALUE),
                ),
                on_delivery=delivery_report,
            )
            self.producer.flush()
        except ValueError:
            print("Invalid input, discarding record...")


class KafkaConsumer:
    def __init__(self, host, port, stream_key, deserializer, **stream_configs) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": host + ":" + str(port),
                "group.id": "mygroup",
                "enable.auto.commit": True,
                "auto.offset.reset": "earliest",
            }
        )
        self.stream_key = stream_key
        self.deserializer = deserializer
        self.running = False
        self.consumer.subscribe([stream_key])

    def consume(self, timeout=1.0):
        self.running = True
        try:
            while self.running:
                new_message = self.consumer.poll(timeout)
                if new_message is None:
                    continue
                if new_message.error():
                    print("Consumer error: {}".format(new_message.error()))
                    continue
                data = self.deserializer(
                    new_message.value(),
                    SerializationContext(self.stream_key, MessageField.VALUE),
                )
                yield data
        finally:
            self.consumer.close()

    def shutdown(self):
        self.running = False


class KafkaStreamFactory:
    def __init__(self, hostname, port) -> None:
        self.hostname = hostname
        self.port = port

    def create_consumer(
        self, stream_key, message_type, deserializer, **stream_configs
    ) -> None:
        if deserializer is None:
            deserializer = DefaultProtobufDeserializer(message_type)
        return KafkaConsumer(
            self.hostname,
            self.port,
            stream_key,
            deserializer,
            **stream_configs,
        )

    def create_producer(
        self, stream_key, message_type, serializer, **stream_configs
    ) -> None:
        if serializer is None:
            serializer = DefaultProtobufSerializer(message_type)
        return KafkaProducer(
            self.hostname, self.port, stream_key, serializer, **stream_configs
        )
