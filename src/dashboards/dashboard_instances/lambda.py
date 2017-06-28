import os

from src.redash_client import RedashClient
from src.utils import read_experiment_definition, format_date
from src.dashboards.ActivityStreamExperimentDashboard import (
    ActivityStreamExperimentDashboard)

DIRECTORY_NAME = "experiments/json_definitions"
FILENAME = "experiments2.json"


def handler(json_input, context):
  api_key = os.environ["REDASH_API_KEY"]
  redash_client = RedashClient(api_key)

  experiments = read_experiment_definition(FILENAME)
  for experiment in experiments:
    dash = ActivityStreamExperimentDashboard(
        redash_client,
        experiment["name"],
        experiment["variant"]["experiment_variant_slug"],
        experiment["addon_versions"],
        format_date(experiment["start_date"]),
        format_date(experiment["end_date"]),
    )

    dash.add_event_graphs(dash.DEFAULT_EVENTS)
    dash.add_event_graphs(
        dash.MASGA_EVENTS, events_table="activity_stream_masga")
    dash.add_events_per_user(dash.DEFAULT_EVENTS)
    dash.add_ttable()
    dash.update_refresh_schedule(43200)
