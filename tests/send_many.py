from app.api_service.rabbit import publish_notification


def build_normal_message(index: int) -> dict:
    packet_loss = round(2.5 + index * 0.3, 2)
    return {
        "event": "packet_loss_alert",
        "source": f"router-{index:02d}",
        "severity": "warning",
        "text": f"Packet loss detected on router-{index:02d}",
        "payload": {
            "packet_loss": packet_loss,
            "threshold": 2.0,
        },
    }


def build_failed_message(index: int) -> dict:
    return {
        "event": "fail",
        "source": f"router-fail-{index:02d}",
        "severity": "error",
        "text": f"Demo failure message from router-fail-{index:02d}",
        "payload": {
            "reason": "demo failure",
        },
    }


def main() -> None:
    for index in range(1, 21):
        publish_notification(build_normal_message(index))

    for index in range(1, 6):
        publish_notification(build_failed_message(index))

    print("Sent 25 messages: 20 normal, 5 failed")


if __name__ == "__main__":
    main()
