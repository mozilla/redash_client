import os
import math
import statistics
from scipy import stats
from utils import upload_as_json
import statsmodels.stats.power as smp
from constants import VizType, ChartType, VizWidth, TtableSchema
from samples.SummaryDashboard import SummaryDashboard
from templates import retention_diff, disable_rate

class ActivityStreamExperimentDashboard(SummaryDashboard):
  TTABLE_DESCRIPTION = "Smaller p-values (e.g. <= 0.05) indicate a high probability that the \
    variants have different distributions. Alpha error indicates the probability a difference is \
    observed when one does not exists. Larger power (e.g. >= 0.7) indicates a high probability \
    that an observed difference is correct. Beta error (1 - power) indicates the probability that no \
    difference is observed when indeed one exists."

  # These are either strings representing both the measurement name
  # event being measured or a key value pair: {<measurement_name>: <events>}
  DEFAULT_EVENTS = ["CLICK", "SEARCH", "BLOCK", "DELETE", "BOOKMARK_ADD",
    {"event_name": "Positive Interactions", "event_list": ['CLICK', 'BOOKMARK_ADD', 'SEARCH']}]
  MASGA_EVENTS = ["HIDE_LOADER", "SHOW_LOADER", "MISSING_IMAGE", "SLOW_ADDON_DETECTED"]
  ALPHA_ERROR = 0.005
  URL_FETCHER_DATA_SOURCE_ID = 28
  DISABLE_TITLE = "Disable Rate"

  def __init__(self, api_key, dash_name, exp_id, addon_versions, start_date=None, end_date=None):
    super(ActivityStreamExperimentDashboard, self).__init__(
      api_key,
      "Activity Stream A/B Testing: " + dash_name,
      "activity_stream_events_daily",
      start_date)

    self._experiment_id = exp_id
    self._end_date = end_date
    self._addon_versions = ", ".join(["'{}'".format(version) for version in addon_versions])
    self.sheets = SheetsClient()

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
                                          ratio=len(exp_vals) / float(len(control_vals)),
                                          alpha=self.ALPHA_ERROR, alternative='two-sided')
    p_val = stats.ttest_ind(control_vals, exp_vals, equal_var = False)[1]
    return power, p_val, exp_mean - control_mean

  def add_disable_graph(self):
    if self.DISABLE_TITLE in self.get_chart_names():
      return

    query_string, fields = disable_rate(self._start_date, self._experiment_id, self._addon_versions)
    query_id, table_id = self.redash.new_query(self.DISABLE_TITLE, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.new_visualization(query_id, VizType.CHART, "", ChartType.LINE, {fields[0]: "x", fields[1]: "y", fields[2]: "series"})
    self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.REGULAR)

  def add_retention_diff(self):
    query_name = "Daily Retention Difference (Experiment - Control)"
    if query_name in self.get_chart_names():
      return

    query_string, fields = retention_diff(self._start_date, self._experiment_id, self._addon_versions)
    query_id, table_id = self.redash.new_query(query_name, query_string, self.TILES_DATA_SOURCE_ID)
    viz_id = self.redash.new_visualization(query_id, VizType.COHORT, time_interval="daily")
    self.redash.append_viz_to_dash(self._dash_id, viz_id, VizWidth.WIDE)

  def get_ttable_data_for_query(self, label, query_string, column_name):
    data = self.redash.get_query_results(query_string, self.TILES_DATA_SOURCE_ID)

    if data is None:
      return []

    control_vals = []
    exp_vals = []
    for row in data:
      if row["type"] == "experiment":
        exp_vals.append(row[column_name])
      else:
        control_vals.append(row[column_name])

    power, p_val, mean_diff = self._power_and_ttest(control_vals, exp_vals)
    return {
      "Metric": label,
      "Alpha Error": self.ALPHA_ERROR,
      "Power": power,
      "Two-Tailed P-value (ttest)": p_val,
      "Experiment Mean - Control Mean": mean_diff
    }

  def add_ttable(self, gservice_email):
    # Don't add a table if it already exists
    query_name = "Statistical Analysis"
    if query_name in self.get_chart_names():
      return

    values = { "columns": TtableSchema, "rows": [] }

    # Create the t-table
    for event in self.DEFAULT_EVENTS + self.MASGA_EVENTS:
      table = self._events_table
      if event in self.MASGA_EVENTS:
        table = "activity_stream_masga"

      event_query_name, query_string, fields = self._get_event_query_data(event, table)
      ttable_row = self.get_ttable_data_for_query(event_query_name, query_string, "event_rate")
      values["rows"].append(ttable_row)

    query_string, fields = disable_rate(self._start_date, self._experiment_id, self._addon_versions)
    disable_ttable_row = self.get_ttable_data_for_query(self.DISABLE_TITLE, query_string, "disable_rate")
    if len(disable_ttable_row) > 0:
      values.append(disable_ttable_row)

    query_string = upload_as_json("experiments", self._experiment_id, values)
    query_id, table_id = self.redash.new_query(query_name, query_string,
      self.URL_FETCHER_DATA_SOURCE_ID, self.TTABLE_DESCRIPTION)
    self.redash.append_viz_to_dash(self._dash_id, table_id, VizWidth.WIDE)
