import os
from constants import RetentionType
from samples.SummaryDashboard import SummaryDashboard

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]

  dash = SummaryDashboard(
    api_key,
    "Firefox iOS: Metrics Summary",
    "activity_stream_mobile_events_daily",
    "02/17/2017"
  )

  dash._events_table = "activity_stream_mobile_stats_daily"
  dash.add_mau_dau()
  dash._events_table = "activity_stream_mobile_events_daily"
  dash.add_retention_graph(RetentionType.DAILY)
  dash.add_events_weekly(event_column="event")
  dash.update_refresh_schedule(3600)
  #dash.remove_all_graphs()
