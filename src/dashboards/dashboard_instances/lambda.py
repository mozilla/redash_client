import os

from src.redash_client import RedashClient
from src.utils import read_experiment_definition, format_date, is_old_date
from src.dashboards.ActivityStreamExperimentDashboard import (
    ActivityStreamExperimentDashboard)

DIRECTORY_NAME = "experiments/json_definitions"
URL = (
    "https://experimenter.dev.mozaws.net/api/v1/"
    "activity-stream/experiments.json?format=json")


def handler(json_input, context):
  api_key = os.environ["REDASH_API_KEY"]
  redash_client = RedashClient(api_key)

  experiments = read_experiment_definition(URL)
  for experiment in experiments:
    end_date = None
    if "end_date" in experiment and experiment["end_date"] is not None:
      if is_old_date(experiment["end_date"]):
        continue

      end_date = format_date(experiment["end_date"])

    dash = ActivityStreamExperimentDashboard(
        redash_client,
        experiment["name"],
        experiment["slug"],
        experiment["addon_versions"],
        format_date(experiment["start_date"]),
        end_date,
    )

    dash.add_graph_templates("AS Template:")
    dash.add_ttable("TTests Template:")
    dash.update_refresh_schedule(43200)
