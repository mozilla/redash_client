import math
import logging

import statistics
from scipy import stats
import statsmodels.stats.power as smp

from src.utils import upload_as_json
from src.constants import (
    VizType, ChartType, VizWidth, TTableSchema, TimeInterval)
from src.dashboards.SummaryDashboard import SummaryDashboard
from src.templates import (
    retention_diff, disable_rate, event_rate, event_per_user)


class ActivityStreamExperimentDashboard(SummaryDashboard):
  TTABLE_DESCRIPTION = (
      "Smaller p-values (e.g. <= 0.05) indicate a high "
      "probability that the variants have different distributions. Alpha "
      "error indicates the probability a difference is observed when one "
      "does not exists. Larger power (e.g. >= 0.7) indicates a high "
      "probability that an observed difference is correct. Beta error "
      "(1 - power) indicates the probability that no difference is observed "
      "when indeed one exists.")

  # These are either strings representing both the measurement name
  # event being measured or a key value pair: {<measurement_name>: <events>}
  DEFAULT_EVENTS = ["CLICK", "SEARCH", "BLOCK", "DELETE", "BOOKMARK_ADD",
                    "CLEAR_HISTORY", {
                        "event_name": "Positive Interactions",
                        "event_list": ["CLICK", "BOOKMARK_ADD", "SEARCH"]}]
  MASGA_EVENTS = ["HIDE_LOADER", "SHOW_LOADER", "MISSING_IMAGE"]
  ALPHA_ERROR = 0.005
  URL_FETCHER_DATA_SOURCE_ID = 28
  DISABLE_TITLE = "Disable Rate"
  RETENTION_DIFF_TITLE = "Daily Retention Difference (Experiment - Control)"
  T_TABLE_TITLE = "Statistical Analysis"
  DASH_PREFIX = "Activity Stream Experiment: {name}"
  EVENT_RATE_MAPPING = {"date": "x", "event_rate": "y", "type": "series"}
  REGULAR_MAPPING = {"date": "x", "event_rate": "y"}
  MASGA_EVENTS_TABLE = "activity_stream_masga"
  SERIES_OPTIONS = {
      "event_rate": {
          "type": ChartType.LINE,
          "yAxis": 0,
          "zIndex": 0,
          "index": 0
      }
  }

  def __init__(self, redash_client, dash_name, exp_id,
               addon_versions, start_date=None, end_date=None):
    super(ActivityStreamExperimentDashboard, self).__init__(
        redash_client,
        self.DASH_PREFIX.format(name=dash_name),
        "activity_stream_events_daily",
        start_date, end_date)

    logging.basicConfig()
    self._logger = logging.getLogger()
    self._logger.setLevel(logging.INFO)
    self._experiment_id = exp_id

    addon_version_list = []
    for version in addon_versions:
      addon_version_list.append("'{}'".format(version))
    self._addon_versions = ", ".join(addon_version_list)

    self._params["addon_versions"] = "(" + self._addon_versions + ")"
    self._params["experiment_id"] = self._experiment_id
    self._logger.info((
        "ActivityStreamExperimentDashboard: {name} "
        "Initialization Complete".format(name=dash_name)))

  def _compute_pooled_stddev(self, control_std, exp_std,
                             control_vals, exp_vals):

    control_len_sub_1 = len(control_vals) - 1
    exp_len_sub_1 = len(exp_vals) - 1

    pooled_stddev_num = (pow(control_std, 2) * control_len_sub_1 +
                         pow(exp_std, 2) * exp_len_sub_1)
    pooled_stddev_denom = control_len_sub_1 + exp_len_sub_1

    pooled_stddev = math.sqrt(pooled_stddev_num / float(pooled_stddev_denom))
    return pooled_stddev

  def _power_and_ttest(self, control_vals, exp_vals):
    control_mean = statistics.mean(control_vals)
    control_std = statistics.stdev(control_vals)
    exp_mean = statistics.mean(exp_vals)
    exp_std = statistics.stdev(exp_vals)

    pooled_stddev = self._compute_pooled_stddev(
        control_std, exp_std, control_vals, exp_vals)

    power = 0
    if control_mean != 0 and pooled_stddev != 0:
      percent_diff = abs(control_mean - exp_mean) / control_mean
      effect_size = (percent_diff * float(control_mean)) / float(pooled_stddev)
      power = smp.TTestIndPower().solve_power(
          effect_size,
          nobs1=len(control_vals),
          ratio=len(exp_vals) / float(len(control_vals)),
          alpha=self.ALPHA_ERROR, alternative='two-sided')

    ttest_result = stats.ttest_ind(control_vals, exp_vals, equal_var=False)
    p_val = ""
    if len(ttest_result) >= 2 and not math.isnan(ttest_result[1]):
      p_val = ttest_result[1]

    mean_diff = exp_mean - control_mean

    if p_val <= 0.05 and mean_diff < 0:
      significance = "Negative"
    elif p_val <= 0.05 and mean_diff > 0:
      significance = "Positive"
    else:
      significance = "Neutral"

    return {
        "power": power,
        "p_val": p_val,
        "mean_diff": mean_diff,
        "significance": significance
    }

  def _get_event_query_data(self, event, event_query=event_rate,
                            events_table=None):
    if events_table is None:
      events_table = self._events_table

    if type(event) == str:
      event_name = event.capitalize()
      event_string = "'{}'".format(event)
    else:
      event_name = event["event_name"]
      events = []
      for event in event["event_list"]:
        events.append("'{}'".format(event))
      event_string = ", ".join(events)

    query_string, fields = event_query(
        event_string,
        self._start_date,
        self._end_date,
        self._experiment_id,
        self._addon_versions,
        events_table)

    query_name = "{0} Rate".format(event_name)
    if event_query != event_rate:
      query_name = "Average {0} Per User".format(event_name)

    return query_name, query_string, fields

  def _get_ttable_data_for_query(self, label, query_string, column_name):
    data = self.redash.get_query_results(
        query_string, self.TILES_DATA_SOURCE_ID)

    if data is None or len(data) <= 3 or (column_name not in data[0]):
      return {}

    control_vals = []
    exp_vals = []
    for row in data:
      if "type" in row and row["type"] == "experiment":
        exp_vals.append(row[column_name])
      elif "type" in row and row["type"] == "control":
        control_vals.append(row[column_name])
      else:
        return {}

    results = self._power_and_ttest(control_vals, exp_vals)
    return {
        "Metric": label,
        "Alpha Error": self.ALPHA_ERROR,
        "Power": results["power"],
        "Two-Tailed P-value (ttest)": results["p_val"],
        "Significance": results["significance"],
        "Experiment Mean - Control Mean": results["mean_diff"]
    }

  def add_disable_graph(self):
    if self.DISABLE_TITLE in self.get_query_ids_and_names():
      return

    query_string, fields = disable_rate(
        self._start_date, self._experiment_id, self._addon_versions)

    mapping = {fields[0]: "x", fields[1]: "y", fields[2]: "series"}

    self._add_query_to_dashboard(
        self.DISABLE_TITLE,
        query_string,
        self.TILES_DATA_SOURCE_ID,
        VizWidth.REGULAR,
        VizType.CHART,
        "",
        ChartType.LINE,
        mapping,
    )

  def add_retention_diff(self):
    if self.RETENTION_DIFF_TITLE in self.get_query_ids_and_names():
      return

    query_string, fields = retention_diff(
        self._start_date, self._experiment_id, self._addon_versions)

    self._add_query_to_dashboard(
        self.RETENTION_DIFF_TITLE,
        query_string,
        self.TILES_DATA_SOURCE_ID,
        VizWidth.WIDE,
        VizType.COHORT,
        time_interval=TimeInterval.DAILY,
    )

  def add_event_graphs(self, events_list, graph_description="",
                       event_query=event_rate, events_table=None):
    self._logger.info(("ActivityStreamExperimentDashboard: "
                       "Adding event graphs with query: "
                       "{query}:".format(query=event_query.__name__)))
    if events_list is None or len(events_list) == 0:
      events_list = self.DEFAULT_EVENTS

    chart_data = self.get_query_ids_and_names()
    for event in events_list:
      GRAPH_DESCRIPTION = graph_description
      if not GRAPH_DESCRIPTION:
        GRAPH_DESCRIPTION = (
            "Percent of sessions with at least "
            "one occurance of {0}")
      GRAPH_DESCRIPTION = GRAPH_DESCRIPTION.format(event)

      query_name, query_string, fields = self._get_event_query_data(
          event, event_query, events_table)

      # Update graphs if they already exist.
      if query_name in chart_data:
        self._logger.info(("ActivityStreamExperimentDashboard: "
                           "{event} event graph exists and is being updated:"
                           .format(event=event)))
        self.redash.update_query(
            chart_data[query_name]["query_id"],
            query_name,
            query_string,
            self.TILES_DATA_SOURCE_ID,
            GRAPH_DESCRIPTION,
        )
        continue

      mapping = {fields[0]: "x", fields[1]: "y", fields[2]: "series"}

      self._logger.info(("ActivityStreamExperimentDashboard: "
                         "{event} event graph is being added:"
                         .format(event=event)))
      self._add_query_to_dashboard(
          query_name,
          query_string,
          self.TILES_DATA_SOURCE_ID,
          VizWidth.REGULAR,
          VizType.CHART,
          GRAPH_DESCRIPTION,
          ChartType.LINE,
          mapping,
      )

  def add_events_per_user(self, events_list, events_table=None):
    GRAPH_DESCRIPTION = ("Average number of {0} events per person per day")
    self.add_event_graphs(events_list, GRAPH_DESCRIPTION, event_per_user)

  def _get_title(self, template_name):
    title = template_name.title().split(": ")
    if len(title) > 1:
      title = title[1]
    return title

  def _apply_event_template(self, template, chart_data,
                            events_list, events_table):
    for event in events_list:
      if type(event) == str:
        event_name = event.capitalize()
        event_string = "('{}')".format(event)
      else:
        event_name = event["event_name"]
        events = []
        for event in event["event_list"]:
          events.append("'{}'".format(event))
        event_string = "(" + ", ".join(events) + ")"

      title = self._get_title(template["name"]).replace(
          "Event", event_name)

      description = template["description"].lower().replace(
          "event", event_name).capitalize()

      self._params["events_table"] = events_table
      self._params["event"] = event_string

      self._add_template(
          template,
          chart_data,
          title,
          self.EVENT_RATE_MAPPING,
          VizWidth.REGULAR,
          description,
      )

  def _add_template(self, template, chart_data, title, mapping,
                    viz_width, description, series_options=None):
    # Remove graphs if they already exist.
    if title in chart_data:
      self._logger.info(("ActivityStreamExperimentDashboard: "
                         "{template} graph exists and is being removed"
                         .format(template=title)))

      query_id = chart_data[title]["query_id"]
      widget_id = chart_data[title]["widget_id"]
      self.remove_graph_from_dashboard(widget_id, query_id)

    self._logger.info(("ActivityStreamExperimentDashboard: "
                       "New {title} graph is being added"
                       .format(title=title)))
    self._add_forked_query_to_dashboard(
        title,
        template["id"],
        self._params,
        viz_width,
        VizType.CHART,
        description,
        ChartType.LINE,
        mapping,
        series_options
    )

  def add_templates(self, templates=[]):
    if len(templates) == 0:
      templates = self.redash.search_queries("AS Template:")

    chart_data = self.get_query_ids_and_names()

    for template in templates:
      if "event" in template["name"].lower():
        self._logger.info((
            "ActivityStreamExperimentDashboard: "
            "Processing template '{template_name}' for default events"
            .format(template_name=template["name"])))
        self._apply_event_template(
            template, chart_data, self.DEFAULT_EVENTS, self._events_table)

        self._logger.info((
            "ActivityStreamExperimentDashboard: "
            "Processing template '{template_name}' for masga events"
            .format(template_name=template["name"])))
        self._apply_event_template(
            template, chart_data, self.MASGA_EVENTS, self.MASGA_EVENTS_TABLE)
      else:
        self._logger.info((
            "ActivityStreamExperimentDashboard: "
            "Processing template '{template_name}'"
            .format(template_name=template["name"])))
        title = description = self._get_title(template["name"])

        if template["description"]:
          description = template["description"]

        self._add_template(
            template,
            chart_data,
            title,
            self.REGULAR_MAPPING,
            VizWidth.WIDE,
            description,
            self.SERIES_OPTIONS
        )

  def add_ttable(self):
    self._logger.info(
        "ActivityStreamExperimentDashboard: Creating a T-Table")

    # Remove a table if it already exists
    widgets = self.get_query_ids_and_names()
    if self.T_TABLE_TITLE in widgets:
      self._logger.info((
          "ActivityStreamExperimentDashboard: "
          "Stale T-Table exists and will be removed"))
      query_id = widgets[self.T_TABLE_TITLE]["query_id"]
      widget_id = widgets[self.T_TABLE_TITLE]["widget_id"]
      self.remove_graph_from_dashboard(widget_id, query_id)

    values = {"columns": TTableSchema, "rows": []}

    # Create the t-table
    for widget_name in widgets:
      query_string = widgets[widget_name]["query"]
      ttable_row = self._get_ttable_data_for_query(
          widget_name, query_string, "event_rate")

      if len(ttable_row) == 0:
        self._logger.info((
            "ActivityStreamExperimentDashboard: "
            "Widget '{name}' has no relevant data and will not be "
            "included in T-Table.".format(name=widget_name)))
        continue

      values["rows"].append(ttable_row)

    query_string = upload_as_json("experiments", self._experiment_id, values)
    query_id, table_id = self.redash.create_new_query(
        self.T_TABLE_TITLE,
        query_string,
        self.URL_FETCHER_DATA_SOURCE_ID,
        self.TTABLE_DESCRIPTION,
    )
    self.redash.add_visualization_to_dashboard(
        self._dash_id, table_id, VizWidth.WIDE)
