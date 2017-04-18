import os
from constants import RetentionType
from samples.TestPilotDashboard import TestPilotDashboard

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  redash_client = RedashClient(api_key)

  test_pilot_experiments = {
    "Summary": "@testpilot-addon",
    "Min Vid": "@min-vid",
    "Cliqz": "testpilot@cliqz.com",
    "Pulse": "pulse@mozilla.com",
    "Snooze Tabs": "snoozetabs@mozilla.com"
  }

  for exp_name in test_pilot_experiments:
    dash = TestPilotDashboard(
      redash_client,
      "Test Pilot: {0}".format(exp_name),
      test_pilot_experiments[exp_name],
      "02/13/2017"
    )

    dash.add_mau_dau()
    dash.add_retention_graph(RetentionType.WEEKLY)
    dash.add_events_weekly()
    dash.update_refresh_schedule(3600)
    #dash.remove_all_graphs()
