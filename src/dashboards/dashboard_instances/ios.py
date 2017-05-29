import os

from src.constants import RetentionType
from src.dashboards.SummaryDashboard import SummaryDashboard


if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  redash_client = RedashClient(api_key)

  dash = SummaryDashboard(
    redash_client,
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
