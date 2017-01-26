import os

from templates import event_rate
from redash_client import RedashClient
from constants import VizType, ChartType, VizWidth

class ActivityStreamExperimentDashboard(object):
  DEFAULT_EVENTS = ["CLICK", "SEARCH", "BLOCK", "DELETE"]
  DATA_SOURCE_ID = 5

  def __init__(self, api_key, dash_name, exp_id, start_date=None, end_date=None):
    self._api_key = api_key
    self._dash_name = "Activity Stream A/B Testing: " + dash_name
    self._experiment_id = exp_id
    self._start_date = start_date
    self._end_date = end_date

    self.redash = RedashClient(api_key)
    self._dash_id = self.redash.new_dashboard(self._dash_name)
    self.redash.publish_dashboard(self._dash_id)

  def add_event_graphs(self, additional_events=[]):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    chart_names = set([widget["name"] for widget in widgets])
    required_events = self.DEFAULT_EVENTS + additional_events

    for event in required_events:
      query_name = "{0} Rate".format(event.capitalize())

      # Don't add graphs that already exist
      if query_name in chart_names:
        continue

      query_string, fields = event_rate(event, self._start_date, self._experiment_id)

      query_id = self.redash.new_query(query_name, query_string, self.DATA_SOURCE_ID)
      viz_id = self.redash.new_visualization(query_id, ChartType.LINE, {fields[0]: "x", fields[1]: "y", fields[2]: "series"})
      self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.WIDE)

  def update_refresh_schedule(self, seconds_to_refresh):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    for widget in widgets:
      self.redash.update_query_schedule(widget["id"], seconds_to_refresh)
