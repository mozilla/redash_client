import os
import math
import statistics
from scipy import stats
from templates import event_rate
import statsmodels.stats.power as smp
from redash_client import RedashClient
from sheets_client import SheetsClient
from constants import VizType, ChartType, VizWidth

class ActivityStreamExperimentDashboard(object):
  TTABLE_DESCRIPTION = "Smaller p-values (e.g. <= 0.05) indicate a high probability that the \
    variants have different distributions. Alpha error indicates the probability a difference is \
    observed when one does not exists. Larger power (e.g. >= 0.7) indicates a high probability \
    that an observed difference is correct. Beta error (1 - power) indicates the probability that no \
    difference is observed when indeed one exists."
  DEFAULT_EVENTS = ["CLICK", "SEARCH", "BLOCK", "DELETE", "BOOKMARK_ADD", "SHARE"]
  ALPHA_ERROR = 0.05
  TILES_DATA_SOURCE_ID = 5
  SHEETS_DATA_SOURCE_ID = 11

  def __init__(self, api_key, dash_name, exp_id, start_date=None, end_date=None):
    self._api_key = api_key
    self._dash_name = "Activity Stream A/B Testing: " + dash_name
    self._experiment_id = exp_id
    self._start_date = start_date
    self._end_date = end_date

    self.redash = RedashClient(api_key)
    self.sheets = SheetsClient()
    self._dash_id = self.redash.new_dashboard(self._dash_name)
    self.redash.publish_dashboard(self._dash_id)

  def _power_and_ttest(self, control_vals, exp_vals):
    control_mean = statistics.mean(control_vals)
    control_std = statistics.stdev(control_vals)
    exp_mean = statistics.mean(exp_vals)
    exp_std = statistics.stdev(exp_vals)

    percent_diff = abs(control_mean - exp_mean) / control_mean
    pooled_stddev = math.sqrt(((pow(control_std, 2) * (len(control_vals) - 1)) + \
                    (pow(exp_std, 2) * (len(exp_vals) - 1))) / \
                    ((len(control_vals) - 1) + (len(exp_vals) - 1)))
    effect_size = (percent_diff * float(control_mean)) / float(pooled_stddev)
    power = smp.TTestIndPower().solve_power(effect_size,
                                          nobs1=len(control_vals),
                                          ratio=len(exp_vals) / len(control_vals),
                                          alpha=self.ALPHA_ERROR, alternative='two-sided')
    p_val = stats.ttest_ind(control_vals, exp_vals, equal_var = False)[1]
    return power, p_val

  def add_event_graphs(self, additional_events=[]):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    chart_names = set([widget["visualization"]["query"]["name"] for widget in widgets])
    required_events = self.DEFAULT_EVENTS + additional_events

    for event in required_events:
      query_name = "{0} Rate".format(event.capitalize())

      # Don't add graphs that already exist
      if query_name in chart_names:
        continue

      query_string, fields = event_rate(event, self._start_date, self._experiment_id)

      query_id, table_id = self.redash.new_query(query_name, query_string, self.TILES_DATA_SOURCE_ID)
      viz_id = self.redash.new_visualization(query_id, ChartType.LINE, {fields[0]: "x", fields[1]: "y", fields[2]: "series"})
      self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.REGULAR)

  def update_refresh_schedule(self, seconds_to_refresh):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    for widget in widgets:
      self.redash.update_query_schedule(widget["visualization"]["query"]["id"], seconds_to_refresh)

  def remove_all_graphs(self):
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    for widget in widgets:
      self.redash.remove_visualization(self._dash_name, widget["id"])

  def add_ttable(self, gservice_email):
    # Don't add a table if it already exists
    query_name = "Statistical Analysis"
    widgets = self.redash.get_widget_from_dash(self._dash_name)
    chart_names = set([widget["visualization"]["query"]["name"] for widget in widgets])
    if query_name in chart_names:
      return

    values = [["Metric", "Alpha Error", "Power", "Two-Tailed P-value (ttest)"]]

    # Create the t-table
    for event in self.DEFAULT_EVENTS:
      query_string, fields = event_rate(event, self._start_date, self._experiment_id)
      data = self.redash.get_query_results(query_string, self.TILES_DATA_SOURCE_ID)

      control_vals = []
      exp_vals = []
      for row in data:
        if row["type"] == "experiment":
          exp_vals.append(row["event_rate"])
        else:
          control_vals.append(row["event_rate"])

      power, p_val = self._power_and_ttest(control_vals, exp_vals)
      values.append(["{0} Rate".format(event.capitalize()), self.ALPHA_ERROR, power, p_val])

    spreadsheet_id = self.sheets.write_to_sheet(self._dash_name, values, gservice_email)
    query_string = "{0}|0".format(spreadsheet_id)
    query_id, table_id = self.redash.new_query(query_name, query_string,
      self.SHEETS_DATA_SOURCE_ID, self.TTABLE_DESCRIPTION)
    self.redash.append_viz_to_dash(self._dash_id, table_id, VizWidth.WIDE)
