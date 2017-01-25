import os

from templates import event_rate
from redash_client import RedashClient
from constants import VizType, ChartType, VizWidth

class ActivityStreamExperimentDashboard(object):
  DEFAULT_EVENTS = ["CLICK", "SEARCH", "BLOCK", "DELETE"]
  DATA_SOURCE_ID = 5

  def __init__(self, api_key, dash_name, exp_id, start_date=None, end_date=None):
    self._api_key = api_key
    self._experiment_id = exp_id
    self._start_date = start_date
    self._end_date = end_date

    self.redash = RedashClient(api_key)
    self._dash_id = self.redash.new_dashboard(dash_name)
    self.redash.publish_dashboard(self._dash_id)

  def add_event_graphs(self, additional_events=[]):
    required_events = self.DEFAULT_EVENTS + additional_events

    for event in required_events:
      query_name = "Experiment vs. Control {0} Rate".format(event)
      query_string, fields = event_rate(event, self._start_date, self._experiment_id)

      query_id = self.redash.new_query(query_name, query_string, self.DATA_SOURCE_ID)
      viz_id = self.redash.new_visualization(query_id, ChartType.LINE, {fields[0]: "x", fields[1]: "y", fields[2]: "series"})
      self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.WIDE)
