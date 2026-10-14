import json
import os
import sys

from google.cloud import bigquery
from google import genai
from google.genai.types import HttpOptions


PROJECT_ID = os.environ["PROJECT_ID"]
DATASET = os.environ.get("DATASET", "streampulse")

TITLE_ID = sys.argv[1] if len(sys.argv) > 1 else "title-northstar"
METRIC_DATE = sys.argv[2] if len(sys.argv) > 2 else ""

bq_client = bigquery.Client(project=PROJECT_ID)

genai_client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location="global",
    http_options=HttpOptions(api_version="v1"),
)


parameters = [
    bigquery.ScalarQueryParameter(
        "title_id",
        "STRING",
        TITLE_ID,
    )
]

date_filter = ""

if METRIC_DATE:
    parameters.append(
        bigquery.ScalarQueryParameter(
            "metric_date",
            "DATE",
            METRIC_DATE,
        )
    )
    date_filter = "AND metric_date = @metric_date"


metrics_query = f"""
SELECT
  metric_date,
  title_id,
  title_name,
  genre,
  qualified_starts,
  completed_sessions,
  completion_rate,
  buffering_seconds,
  viewing_seconds,
  buffering_rate
FROM `{PROJECT_ID}.{DATASET}.title_daily_metrics_enriched`
WHERE title_id = @title_id
{date_filter}
ORDER BY metric_date DESC
LIMIT 30
"""

metrics_job_config = bigquery.QueryJobConfig(
    query_parameters=parameters
)

metrics_rows = [
    dict(row.items())
    for row in bq_client.query(
        metrics_query,
        job_config=metrics_job_config,
    ).result()
]


catalog_query = f"""
SELECT
  metric_name,
  definition,
  grain,
  exclusions,
  owner
FROM `{PROJECT_ID}.{DATASET}.metric_catalog`
ORDER BY metric_name
"""

catalog_rows = [
    dict(row.items())
    for row in bq_client.query(catalog_query).result()
]


trusted_context = {
    "metric_catalog": catalog_rows,
    "metrics": metrics_rows,
}


prompt = f"""
You are StreamPulse's governed, read-only analytics assistant.

Answer using only the TRUSTED_CONTEXT below.

Rules:
1. Never invent metric values.
2. Never calculate a value from missing data.
3. If no matching data exists, clearly say that no matching record was found.
4. Use the metric catalog definitions when explaining metrics.
5. Keep the answer concise and suitable for a business stakeholder.
6. Mention the metric date and title when available.
7. State that the data comes from the trusted BigQuery metrics view.

TRUSTED_CONTEXT:
{json.dumps(trusted_context, default=str, indent=2)}

USER REQUEST:
Give me the latest trusted metrics for title_id '{TITLE_ID}'
{f"on date '{METRIC_DATE}'" if METRIC_DATE else ""}
"""

response = genai_client.models.generate_content(
    model="gemini-2.5-flash",
    contents=prompt,
)

print(response.text)
