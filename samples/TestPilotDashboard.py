from samples.SummaryDashboard import SummaryDashboard

class TestPilotDashboard(SummaryDashboard):
  def __init__(self, api_key, dash_name, addon_id, start_date):
    super(TestPilotDashboard, self).__init__(
      api_key,
      dash_name,
      "ping_centre_test_pilot",
      start_date)

    self._addon_id = addon_id
    self._where_clause = "AND addon_id = '{0}'".format(self._addon_id)

  def add_retention_graph(self, retention_type):
    super(TestPilotDashboard, self).add_retention_graph(
      retention_type,
      self._where_clause)

  def add_events_weekly(self):
    super(TestPilotDashboard, self).add_events_weekly(
      self._where_clause)

  def add_mau_dau(self):
    super(TestPilotDashboard, self).add_mau_dau(
      self._where_clause)