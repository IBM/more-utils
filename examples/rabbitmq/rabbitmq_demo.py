from more_utils.messaging import RabbitMQFactory
import argparse
import json


class ServerMessageHandler:
    def __init(self):
        self.message = None

    def handler(self, message):
        self.message = json.loads(message)


parser = argparse.ArgumentParser(description="Messaging Client")
parser.add_argument("--credentials", required=True)
parser.add_argument("--broker_user", help="Defaults to credentials file")
parser.add_argument("--broker_password", help="Defaults to credentials file")
args_params = parser.parse_args()

rabbitmq_context = RabbitMQFactory.create_context(args=args_params)
with rabbitmq_context.client("feeds", "replies") as client:
    publisher = client.get_publisher()
    message = {"action": "fromClient", "payload": "client data"}
    publisher.publish(json.dumps(message))
    print("CLIENT MESSAGE SENT: ", message)

    with rabbitmq_context.client("replies", "feeds") as server:
        receiver = server.get_receiver()
        mh = ServerMessageHandler()
        message = receiver.receive(mh.handler, max_messages=1)
        message = message.decode("UTF-8")
        message = json.loads(message)
        print("SERVER RECEIVED: ", message)

        publisher = server.get_publisher()
        message = {
            "action": "fromServer",
            "payload": message["payload"] + " / server data",
        }
        publisher.publish(json.dumps(message))
        print("SERVER MESSAGE SENT: ", message)

    receiver = client.get_receiver()
    message = receiver.receive(max_messages=1, timeout=100)
    message = message.decode("UTF-8")
    message = json.loads(message)
    print("CLIENT RECEIVED: ", message)
