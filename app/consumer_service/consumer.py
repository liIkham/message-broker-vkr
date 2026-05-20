import json
import os
import time

import pika


RETRY_BACKOFF_SECONDS = {
    1: 1,
    2: 3,
    3: 5,
}


def get_connection() -> pika.BlockingConnection:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASS", "guest")
    credentials = pika.PlainCredentials(username=user, password=password)
    parameters = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
    return pika.BlockingConnection(parameters)


def _extract_retry_count(properties: pika.spec.BasicProperties | None) -> int:
    if properties is None or properties.headers is None:
        return 0
    value = properties.headers.get("x-retry-count", 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _build_headers(properties: pika.spec.BasicProperties | None) -> dict:
    if properties is None or properties.headers is None:
        return {}
    return dict(properties.headers)


def on_message(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    method: pika.spec.Basic.Deliver,
    properties: pika.spec.BasicProperties,
    body: bytes,
) -> None:
    retry_count = _extract_retry_count(properties)
    headers = _build_headers(properties)

    try:
        payload = json.loads(body.decode("utf-8"))
        if payload.get("event") == "fail":
            raise Exception("Simulated failure")
        print("SUCCESS")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        if retry_count < 3:
            next_retry = retry_count + 1
            delay = RETRY_BACKOFF_SECONDS.get(next_retry, 5)
            time.sleep(delay)
            headers["x-retry-count"] = next_retry
            channel.basic_publish(
                exchange="",
                routing_key="notifications",
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers=headers,
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print(f"RETRY {next_retry}")
        else:
            channel.basic_publish(
                exchange="",
                routing_key="notifications.dlq",
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers=headers,
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            print("DLQ")


def run_consumer() -> None:
    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue="notifications", durable=True)
    channel.queue_declare(queue="notifications.dlq", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="notifications", on_message_callback=on_message)
    channel.start_consuming()


if __name__ == "__main__":
    run_consumer()
