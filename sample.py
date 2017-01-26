import os
from samples.ActivityStreamExperimentDashboard import ActivityStreamExperimentDashboard

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  dash = ActivityStreamExperimentDashboard(
    api_key,
    "Deduped Combined Frecency",
    "exp-006-deduped-combined-frecency",
    "01/16/17"
  )
  dash.add_event_graphs()
  dash.update_refresh_schedule(900)
