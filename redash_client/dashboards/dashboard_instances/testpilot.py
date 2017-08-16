import os

from redash_client.constants import RetentionType
from redash_client.dashboards.SummaryDashboard import SummaryDashboard


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
    where_clause = "AND addon_id = '{0}'".format(test_pilot_experiments[exp_name])

    dash = SummaryDashboard(
      redash_client,
      "Test Pilot: {0}".format(exp_name),
      "ping_centre_test_pilot"
      "02/13/2017"
    )

    dash.add_mau_dau(where_clause)
    dash.add_retention_graph(RetentionType.WEEKLY, where_clause)
    dash.add_events_weekly(where_clause)
    dash.update_refresh_schedule(3600)
    #dash.remove_all_graphs()
