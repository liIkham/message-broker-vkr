import json
import os

import pika


def publish_notification(message: dict) -> None:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    try:
        channel = connection.channel()
        channel.queue_declare(queue="notifications", durable=True)
        body = json.dumps(message).encode("utf-8")
        channel.basic_publish(
            exchange="",
            routing_key="notifications",
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        connection.close()


if __name__ == "__main__":

    test_message = {
    "event": "fail",
    "source": "cli",
    "text": "RabbitMQ publish test",
    }
    
   
    publish_notification(test_message)
    print("Test message published to notifications queue")
