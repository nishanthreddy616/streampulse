import argparse
import json
import uuid
from datetime import datetime, timezone
from apache_beam.utils.timestamp import Timestamp
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


REQUIRED_FIELDS = {
    "event_id",
    "schema_version",
    "event_type",
    "occurred_at",
    "session_id",
    "title_id",
    "region",
    "device_type",
}

ALLOWED_EVENT_TYPES = {
    "play",
    "pause",
    "buffer",
    "complete",
}
VALID_SCHEMA = {
    "fields": [
        {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "schema_version", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "occurred_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "title_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "region", "type": "STRING", "mode": "REQUIRED"},
        {"name": "device_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "duration_seconds", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

REJECTED_SCHEMA = {
    "fields": [
        {"name": "reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "payload", "type": "STRING", "mode": "NULLABLE"},
        {"name": "rejected_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

class ValidateEvent(beam.DoFn):
    def process(self, raw_message):
        try:
            event = json.loads(raw_message.decode("utf-8"))

            missing_fields = REQUIRED_FIELDS - event.keys()
            if missing_fields:
                raise ValueError(
                    f"missing fields: {sorted(missing_fields)}"
                )

            uuid.UUID(event["event_id"])

            if event["schema_version"] != 1:
                raise ValueError("unsupported schema_version")

            if event["event_type"] not in ALLOWED_EVENT_TYPES:
                raise ValueError("unsupported event_type")

            if event["device_type"] not in {"web", "mobile", "tv"}:
                raise ValueError("unsupported device_type")

            if not isinstance(event["duration_seconds"], (int, float)):
                raise ValueError("duration_seconds must be numeric")

            if not 0 <= event["duration_seconds"] <= 21600:
                raise ValueError("duration_seconds is outside the allowed range")

            event["occurred_at"] = beam_timestamp(event["occurred_at"])
            event["ingested_at"] = beam_timestamp(datetime.now(timezone.utc))

            yield beam.pvalue.TaggedOutput("valid", event)

        except Exception as error:
            yield beam.pvalue.TaggedOutput(
                "rejected",
                {
                    "reason": str(error),
                    "payload": raw_message.decode(
                        "utf-8",
                        errors="replace",
                    )[:4096],
                    "rejected_at": beam_timestamp(datetime.now(timezone.utc)),
                },
            )
def beam_timestamp(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    return Timestamp.from_utc_datetime(parsed)

def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        required=True,
    )

    parser.add_argument(
        "--events_table",
        required=True,
    )

    parser.add_argument(
        "--rejected_table",
        required=True,
    )

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        results = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=known_args.input_subscription
            )
            | "Validate events"
            >> beam.ParDo(ValidateEvent()).with_outputs(
                "valid",
                "rejected",
            )
        )

        (
            results.valid
            | "Write valid events"
            >> beam.io.WriteToBigQuery(
                known_args.events_table,
                schema=VALID_SCHEMA,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        (
            results.rejected
            | "Write rejected events"
            >> beam.io.WriteToBigQuery(
                known_args.rejected_table,
                schema=REJECTED_SCHEMA,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
