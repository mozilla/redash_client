from redash_client import RedashClient
from templates import retention, all_events_weekly, active_users
from constants import VizWidth, VizType, RetentionType, ChartType

class SummaryDashboard(object):
  TILES_DATA_SOURCE_ID = 5
  DAILY_RETENTION_TITLE = "Daily Retention"
  EVENTS_WEEKLY_TITLE = "Weely Events"
  MAU_DAU_TITLE = "Engagement"

  def __init__(self, api_key, dash_name, events_table_name, start_date):
    self._api_key = api_key
    self._dash_name = dash_name
    self._events_table = events_table_name
    self._start_date = start_date

    self.redash = RedashClient(api_key)
    self._dash_id = self.redash.create_new_dashboard(self._dash_name)
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
      self.redash.remove_visualization(widget["id"])
      self.redash.delete_query(widget["visualization"]["query"]["id"])

  def add_mau_dau(self, where_clause=""):
    if self.MAU_DAU_TITLE in self.get_chart_names():
      return

    series_options = {
      "mau": {
        "type": ChartType.AREA,
        "yAxis": 0,
        "zIndex":0,
        "index":0
      },
      "wau": {
        "type": ChartType.AREA,
        "yAxis": 0,
        "zIndex": 1,
        "index":0
      },
      "dau": {
        "type": ChartType.AREA,
        "yAxis": 0,
        "zIndex": 2,
        "index":0
      },
    }

    query_string, fields = active_users(self._events_table, self._start_date, where_clause)
    query_id, table_id = self.redash.create_new_query(self.MAU_DAU_TITLE, query_string, self.TILES_DATA_SOURCE_ID)

    # Make the MAU/WAU/DAU graph
    viz_id = self.redash.create_new_visualization(query_id, VizType.CHART, "",
      ChartType.AREA, {fields[0]: "x", fields[1]: "y", fields[2]: "y", fields[3]: "y"}, series_options=series_options)
    self.redash.add_visualization_to_dashboard(self._dash_id, viz_id, VizWidth.WIDE)

    # Make the engagement ratio graph
    viz_id = self.redash.create_new_visualization(query_id, VizType.CHART, "",
      ChartType.LINE, {fields[0]: "x", fields[4]: "y", fields[5]: "y"})
    self.redash.add_visualization_to_dashboard(self._dash_id, viz_id, VizWidth.WIDE)

  def add_retention_graph(self, retention_type, where_clause=""):
    if self.DAILY_RETENTION_TITLE in self.get_chart_names():
      return

    if retention_type == RetentionType.DAILY:
      time_interval = "daily"

    time_interval = "daily" if retention_type == RetentionType.DAILY else "weekly"

    query_string, fields = retention(self._events_table, retention_type, self._start_date, where_clause)
    query_id, table_id = self.redash.create_new_query(self.DAILY_RETENTION_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.create_new_visualization(query_id, VizType.COHORT, time_interval=time_interval)
    self.redash.add_visualization_to_dashboard(self._dash_id, viz_id, VizWidth.WIDE)

  def add_events_weekly(self, where_clause="", event_column="event_type"):
    if self.EVENTS_WEEKLY_TITLE in self.get_chart_names():
      return

    query_string, fields = all_events_weekly(self._events_table, self._start_date, where_clause, event_column)
    query_id, table_id = self.redash.create_new_query(self.EVENTS_WEEKLY_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.create_new_visualization(query_id, VizType.CHART, "",
      ChartType.BAR, {fields[0]: "x", fields[1]: "y", fields[2]: "series"}, stacking=True)
    self.redash.add_visualization_to_dashboard(self._dash_id, viz_id, VizWidth.WIDE)
