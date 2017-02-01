import os
from samples.ActivityStreamExperimentDashboard import ActivityStreamExperimentDashboard

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  gservice_email = os.environ["GSERVICE_EMAIL"]
  dash = ActivityStreamExperimentDashboard(
    api_key,
    "Deduped Combined Frecency",
    "exp-006-deduped-combined-frecency",
    "01/18/17"
  )

  dash.add_event_graphs()
  dash.add_ttable(gservice_email)
  dash.update_refresh_schedule(900)
  #dash.remove_all_graphs()
