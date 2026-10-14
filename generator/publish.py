import json
import os
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

project_id = os.environ["PROJECT_ID"]
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, "streampulse-playback")
now = datetime.now(timezone.utc)


def make_event(
    event_type,
    session_id,
    title_id,
    duration_seconds=0,
    event_id=None,
    occurred_at=None,
):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "schema_version": 1,
        "event_type": event_type,
        "occurred_at": (occurred_at or now).isoformat().replace("+00:00", "Z"),
        "session_id": session_id,
        "title_id": title_id,
        "region": "US",
        "device_type": "tv",
        "duration_seconds": duration_seconds,
    }


duplicate_id = str(uuid.uuid4())

events = [
    make_event(
        "play",
        "session-001",
        "title-northstar",
        duration_seconds=120,
        event_id=duplicate_id,
    ),
    make_event(
        "buffer",
        "session-001",
        "title-northstar",
        duration_seconds=12,
    ),
    make_event(
        "complete",
        "session-001",
        "title-northstar",
    ),
    make_event(
        "play",
        "session-001",
        "title-northstar",
        duration_seconds=120,
        event_id=duplicate_id,
    ),
    make_event(
        "play",
        "session-late",
        "title-afterglow",
        duration_seconds=45,
        occurred_at=now - timedelta(hours=3),
    ),
    {
        "event_id": "bad-event",
        "schema_version": 99,
        "event_type": "wrong",
    },
]


for event in events:
    publisher.publish(
        topic_path,
        json.dumps(event).encode("utf-8"),
    ).result()

    print("published", event.get("event_type"), event.get("event_id"))

print(f"Published {len(events)} synthetic events")
