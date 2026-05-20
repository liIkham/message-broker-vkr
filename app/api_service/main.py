from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI
from pydantic import BaseModel, Field

from app.api_service.rabbit import publish_notification


app = FastAPI()


class NotificationRequest(BaseModel):
    event: str
    source: str
    severity: str | None = "info"
    text: str
    payload: dict | None = Field(default_factory=dict)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/notify")
def notify(notification: NotificationRequest) -> dict:
    message_id = str(uuid4())
    message = {
        "id": message_id,
        "event": notification.event,
        "source": notification.source,
        "severity": notification.severity,
        "text": notification.text,
        "payload": notification.payload if notification.payload is not None else {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    publish_notification(message)
    return {"status": "queued", "id": message_id}
