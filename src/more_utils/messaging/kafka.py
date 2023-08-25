import io
from uuid import uuid4
import json
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import (
    Deserializer,
    MessageField,
    SerializationContext,
    SerializationError,
    Serializer,
    StringSerializer,
    StringDeserializer,
)
from google.protobuf.message_factory import MessageFactory

from more_utils.logging import configure_logger

LOGGER = configure_logger(logger_name="Kafka")


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        LOGGER.error("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    LOGGER.debug(
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


class JSONSerializer(Serializer):
    def __call__(self, message, ctx: SerializationContext):
        if message is None:
            return None

        if not isinstance(message, dict):
            raise ValueError("message must be of type dict")

        with _ContextStringIO() as fo:
            fo.write(StringSerializer("utf8")(json.dumps(message)))
            return fo.getvalue()


class JSONDeserializer(Deserializer):
    def __call__(self, data, ctx: SerializationContext):
        with _ContextStringIO(data) as payload:
            try:
                message = json.loads(StringDeserializer("utf8")(payload.read()))
                return message

            except Exception as e:
                raise SerializationError(
                    f"Failed to decode payload with message type: {type(data)}" + str(e)
                )


class ProtobufSerializer(Serializer):
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


class ProtobufDeserializer(Deserializer):
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
    def __init__(self, host, port, stream_key_and_serializer, **stream_configs) -> None:
        self.stream_key_and_serializer = stream_key_and_serializer
        self.producer = Producer({"bootstrap.servers": host + ":" + str(port)})

    def produce(self, data, stream_key, partition=0):
        try:
            self.producer.produce(
                topic=stream_key,
                partition=partition,
                key=StringSerializer("utf8")(str(uuid4())),
                value=self.stream_key_and_serializer[stream_key](
                    data,
                    SerializationContext(stream_key, MessageField.VALUE),
                ),
                on_delivery=delivery_report,
            )
            self.producer.flush()
        except ValueError as e:
            LOGGER.error(f"Invalid input, discarding record. {e}")


class KafkaConsumer:
    def __init__(
        self, host, port, stream_key_and_deserializer: dict, **stream_configs
    ) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": host + ":" + str(port),
                "group.id": "mygroup",
                "enable.auto.commit": True,
                "auto.offset.reset": "earliest",
                "on_commit": self.commit_completed,
            }
        )
        self.stream_key_and_deserializer = stream_key_and_deserializer
        self.running = False
        self.consumer.subscribe(list(stream_key_and_deserializer.keys()))

    def commit_completed(self, err, partitions):
        if err:
            LOGGER.error(str(err))
        else:
            LOGGER.debug("Committed partition offsets: " + str(partitions))

    def consume(self, timeout=1.0):
        self.running = True
        while self.running:
            new_message = self.consumer.poll(timeout)
            if new_message is None:
                continue
            if new_message.error():
                LOGGER.error("Consumer error: {}".format(new_message.error()))
                continue
            data = self.stream_key_and_deserializer[new_message.topic()](
                new_message.value(),
                SerializationContext(
                    self.stream_key_and_deserializer.keys(), MessageField.VALUE
                ),
            )
            yield data, new_message.topic()

    def shutdown(self):
        self.running = False
        self.consumer.close()


class KafkaStreamFactory:
    def __init__(self, hostname, port) -> None:
        self.hostname = hostname
        self.port = port

    def create_consumer(self, stream_key_and_deserializer, **stream_configs) -> None:
        return KafkaConsumer(
            self.hostname,
            self.port,
            stream_key_and_deserializer,
            **stream_configs,
        )

    def create_producer(self, stream_key_and_serializer, **stream_configs) -> None:
        return KafkaProducer(
            self.hostname, self.port, stream_key_and_serializer, **stream_configs
        )
