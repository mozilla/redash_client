from templates import retention, all_events_weekly, active_users
from constants import VizWidth, VizType, RetentionType, ChartType, TimeInterval


class SummaryDashboard(object):
  TILES_DATA_SOURCE_ID = 5
  DAILY_RETENTION_TITLE = "Daily Retention"
  EVENTS_WEEKLY_TITLE = "Weely Events"
  MAU_DAU_TITLE = "Engagement"
  MAU_DAU_SERIES_OPTIONS = {
      "mau": {
          "type": ChartType.AREA,
          "yAxis": 0,
          "zIndex": 0,
          "index": 0
      },
      "wau": {
          "type": ChartType.AREA,
          "yAxis": 0,
          "zIndex": 1,
          "index": 0
      },
      "dau": {
          "type": ChartType.AREA,
          "yAxis": 0,
          "zIndex": 2,
          "index": 0
      },
  }

  class SummaryDashboardException(Exception):
    pass

  def __init__(self, redash_client, dash_name, events_table_name, start_date):
    self._dash_name = dash_name
    self._events_table = events_table_name
    self._start_date = start_date

    self.redash = redash_client
    self._dash_id = self.redash.create_new_dashboard(self._dash_name)
    self.redash.publish_dashboard(self._dash_id)

  def update_refresh_schedule(self, seconds_to_refresh):
    widgets = self.redash.get_widget_from_dash(self._dash_name)

    for widget in widgets:
      widget_id = widget.get(
          "visualization", {}).get("query", {}).get("id", None)

      if not widget_id:
        continue

      self.redash.update_query_schedule(widget_id, seconds_to_refresh)

  def get_chart_names(self):
    widgets = self.redash.get_widget_from_dash(self._dash_name)

    chart_names = set()
    for widget in widgets:
      widget_name = widget.get(
          "visualization", {}).get("query", {}).get("name", None)

      if not widget_name:
        continue

      chart_names.add(widget_name)

    return chart_names

  def remove_all_graphs(self):
    widgets = self.redash.get_widget_from_dash(self._dash_name)

    for widget in widgets:
      widget_id = widget.get("id", None)
      if widget_id is not None:
        self.redash.remove_visualization(widget_id)

      query_id = widget.get(
          "visualization", {}).get("query", {}).get("id", None)
      if query_id is not None:
        self.redash.delete_query(query_id)

  def _get_mau_dau_column_mappings(self, query_fields):
    mau_dau_column_mapping = {
        # Date
        query_fields[0]: "x",
        # DAU
        query_fields[1]: "y",
        # WAU
        query_fields[2]: "y",
        # MAU
        query_fields[3]: "y",
    }
    engagement_ratio_column_mapping = {
        # Date
        query_fields[0]: "x",
        # Weekly Engagement
        query_fields[4]: "y",
        # Montly Engagement
        query_fields[5]: "y",
    }
    return mau_dau_column_mapping, engagement_ratio_column_mapping

  def add_mau_dau(self, where_clause=""):
    if self.MAU_DAU_TITLE in self.get_chart_names():
      return

    query_string, fields = active_users(
        self._events_table, self._start_date, where_clause)
    query_id, table_id = self.redash.create_new_query(
        self.MAU_DAU_TITLE, query_string, self.TILES_DATA_SOURCE_ID)

    mau_dau_mapping, er_mapping = self._get_mau_dau_column_mappings(fields)

    # Make the MAU/WAU/DAU graph
    viz_id = self.redash.create_new_visualization(
        query_id,
        VizType.CHART,
        "",
        ChartType.AREA,
        mau_dau_mapping,
        series_options=self.MAU_DAU_SERIES_OPTIONS)
    self.redash.add_visualization_to_dashboard(
        self._dash_id, viz_id, VizWidth.WIDE)

    # Make the engagement ratio graph
    viz_id = self.redash.create_new_visualization(
        query_id, VizType.CHART, "", ChartType.LINE, er_mapping)
    self.redash.add_visualization_to_dashboard(
        self._dash_id, viz_id, VizWidth.WIDE)

  def add_retention_graph(self, retention_type, where_clause=""):
    if self.DAILY_RETENTION_TITLE in self.get_chart_names():
      return

    time_interval = TimeInterval.WEEKLY
    if retention_type == RetentionType.DAILY:
      time_interval = TimeInterval.DAILY

    query_string, fields = retention(
        self._events_table, retention_type, self._start_date, where_clause)
    query_id, table_id = self.redash.create_new_query(
        self.DAILY_RETENTION_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.create_new_visualization(
        query_id, VizType.COHORT, time_interval=time_interval)
    self.redash.add_visualization_to_dashboard(
        self._dash_id, viz_id, VizWidth.WIDE)

  def add_events_weekly(self, where_clause="", event_column="event_type"):
    if self.EVENTS_WEEKLY_TITLE in self.get_chart_names():
      return

    query_string, fields = all_events_weekly(
        self._events_table, self._start_date, where_clause, event_column)

    column_mapping = {
        fields[0]: "x",
        fields[1]: "y",
        fields[2]: "series",
    }

    query_id, table_id = self.redash.create_new_query(
        self.EVENTS_WEEKLY_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.create_new_visualization(
        query_id,
        VizType.CHART,
        "",
        ChartType.BAR,
        column_mapping,
        stacking=True,
    )
    self.redash.add_visualization_to_dashboard(
        self._dash_id, viz_id, VizWidth.WIDE)
