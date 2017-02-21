from redash_client import RedashClient
from templates import retention, all_events_weekly
from constants import VizWidth, VizType, RetentionType, ChartType

class SummaryDashboard(object):
  TILES_DATA_SOURCE_ID = 5
  DAILY_RETENTION_TITLE = "Daily Retention"
  EVENTS_WEEKLY_TITLE = "Weely Events"

  def __init__(self, api_key, dash_name, events_table_name, start_date):
    self._api_key = api_key
    self._dash_name = dash_name
    self._events_table = events_table_name
    self._start_date = start_date

    self.redash = RedashClient(api_key)
    self._dash_id = self.redash.new_dashboard(self._dash_name)
    self.redash.publish_dashboard(self._dash_id)

  def update_refresh_schedule(self, seconds_to_refresh):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    for widget in widgets:
      self.redash.update_query_schedule(widget["visualization"]["query"]["id"], seconds_to_refresh)

  def get_chart_names(self):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    return set([widget["visualization"]["query"]["name"] for widget in widgets])

  def remove_all_graphs(self):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    for widget in widgets:
      self.redash.remove_visualization(self._dash_name, widget["id"])

  def add_retention_graph(self, retention_type, start_date, where_clause):
    if self.DAILY_RETENTION_TITLE in self.get_chart_names():
      return

    time_interval = "daily" if retention_type == RetentionType.DAILY else "weekly"

    query_string, fields = retention(self._events_table, retention_type, start_date, where_clause)
    query_id, table_id = self.redash.new_query(self.DAILY_RETENTION_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.new_visualization(query_id, VizType.COHORT, time_interval=time_interval)
    self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.WIDE)

  def add_events_weekly(self, start_date, where_clause):
    if self.EVENTS_WEEKLY_TITLE in self.get_chart_names():
      return

    query_string, fields = all_events_weekly(self._events_table, start_date, where_clause)
    query_id, table_id = self.redash.new_query(self.EVENTS_WEEKLY_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.new_visualization(query_id, VizType.CHART, "",
      ChartType.BAR, {fields[0]: "x", fields[1]: "y", fields[2]: "series"}, stacking=True)
    self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.WIDE)
