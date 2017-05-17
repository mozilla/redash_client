import math
import mock
import json
import statistics

from src.templates import event_rate
from src.tests.base import AppTest
from samples.ActivityStreamExperimentDashboard import (
    ActivityStreamExperimentDashboard)


class TestActivityStreamExperimentDashboard(AppTest):

  ADDON_VERSIONS = ["1.8.0", "1.9.0"]
  START_DATE = "02/17/2017"
  DASH_PREFIX = "Activity Stream A/B Testing: {0}"
  DASH_NAME = "Screenshots Long Cache"
  EXPERIMENT_ID = "exp-014-screenshotsasync"

  def get_dashboard(self, api_key):
    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.return_value = self.get_mock_response()

    dashboard = ActivityStreamExperimentDashboard(
        self.redash,
        self.DASH_NAME,
        self.EXPERIMENT_ID,
        self.ADDON_VERSIONS,
        self.START_DATE,
    )
    return dashboard

  def test_correct_values_at_initialization(self):
    self.assertEqual(self.dash._experiment_id, self.EXPERIMENT_ID)
    self.assertEqual(
        self.dash._dash_name, self.DASH_PREFIX.format(self.DASH_NAME))
    self.assertEqual(self.dash._start_date, self.START_DATE)
    self.assertEqual(self.dash._addon_versions, "'1.8.0', '1.9.0'")

    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_pooled_stddev(self):
    exp_vals = [1, 2, 3]
    control_vals = [4, 6, 8]
    EXPECTED_POOLED_STDDEV = math.sqrt(10 / float(4))

    exp_std = statistics.stdev(exp_vals)
    control_std = statistics.stdev(control_vals)

    pooled_stddev = self.dash._compute_pooled_stddev(
        control_std, exp_std, control_vals, exp_vals)

    self.assertEqual(pooled_stddev, EXPECTED_POOLED_STDDEV)

  def test_power_and_ttest_correct_results(self):
    exp_vals = [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3]
    control_vals = [4, 6, 8, 4, 6, 8, 4, 6, 8, 4, 6, 8]
    MEAN_DIFFERENCE = -4

    power, p_val, mean_diff = self.dash._power_and_ttest(
        control_vals, exp_vals)

    self.assertEqual(mean_diff, MEAN_DIFFERENCE)
    self.assertTrue(0 <= p_val <= 0.05)
    self.assertTrue(0.5 <= power <= 1)

  def test_get_query_event_data_for_string(self):
    EVENT = "CLICK"
    EXPECTED_GRAPH_TITLE = "Click Rate"
    EXPECTED_QUERY_STRING, EXPECTED_FIELDS = event_rate(
        "'{}'".format(EVENT),
        self.START_DATE,
        self.EXPERIMENT_ID,
        self.dash._addon_versions,
        self.dash._events_table)

    query_name, query_string, fields = self.dash._get_event_query_data(
        EVENT)

    self.assertEqual(query_name, EXPECTED_GRAPH_TITLE)
    self.assertEqual(query_string, EXPECTED_QUERY_STRING)
    self.assertEqual(len(fields), 3)
    for i in xrange(len(fields)):
      self.assertEqual(fields[i], EXPECTED_FIELDS[i])

  def test_get_query_event_data_for_dict(self):
    EVENT = {
        "event_name": "Positive Interactions",
        "event_list": ["CLICK", "BOOKMARK_ADD", "SEARCH"]
    }
    EVENT_LIST_STRING = "'CLICK', 'BOOKMARK_ADD', 'SEARCH'"
    EXPECTED_GRAPH_TITLE = "Positive Interactions Rate"
    EXPECTED_QUERY_STRING, EXPECTED_FIELDS = event_rate(
        EVENT_LIST_STRING,
        self.START_DATE,
        self.EXPERIMENT_ID,
        self.dash._addon_versions,
        self.dash._events_table)

    query_name, query_string, fields = self.dash._get_event_query_data(
        EVENT)

    self.assertEqual(query_name, EXPECTED_GRAPH_TITLE)
    self.assertEqual(query_string, EXPECTED_QUERY_STRING)
    self.assertEqual(len(fields), 3)
    for i in xrange(len(fields)):
      self.assertEqual(fields[i], EXPECTED_FIELDS[i])

  def test_get_ttable_data_for_non_existent_query(self):
    QUERY_RESULTS_RESPONSE = {}

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_RESPONSE))

    ttable_row = self.dash._get_ttable_data_for_query(
        "beep", "meep", "boop")

    self.assertEqual(ttable_row, {})

  def test_ttable_row_data_is_correct(self):
    EXPECTED_LABEL = "beep"
    EXPECTED_ROWS = []
    EXPECTED_MEAN_DIFFERENCE = -4

    for i in xrange(12):
      EXPECTED_ROWS.append({
          "date": 123,
          "event_rate": (i % 3) + 1,
          "type": "experiment"
      })
      EXPECTED_ROWS.append({
          "date": 123,
          "event_rate": ((i * 2) % 6) + 4,  # 4, 6, 8
          "type": "control"
      })

    QUERY_RESULTS_RESPONSE = {
        "query_result": {
            "data": {
                "rows": EXPECTED_ROWS
            }
        }
    }

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_RESPONSE))

    ttable_row = self.dash._get_ttable_data_for_query(
        EXPECTED_LABEL, "meep", "event_rate")

    self.assertEqual(len(ttable_row), 5)
    self.assertEqual(ttable_row["Metric"], EXPECTED_LABEL)
    self.assertEqual(ttable_row["Alpha Error"], self.dash.ALPHA_ERROR)
    self.assertTrue(0.5 <= ttable_row["Power"] <= 1)
    self.assertTrue(0 <= ttable_row["Two-Tailed P-value (ttest)"] <= 0.05)
    self.assertEqual(
        ttable_row["Experiment Mean - Control Mean"], EXPECTED_MEAN_DIFFERENCE)

  def test_add_disable_graph_makes_correct_calls(self):
    self.server_calls = 0

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = self.post_server

    self.dash.add_disable_graph()

    # GET calls:
    #     1) Create dashboard
    #     2) Get dashboard widgets
    #     3) Get table ID
    # POST calls:
    #     1) Create dashboard
    #     2) Create query
    #     3) Refresh query
    #     4) Create visualization
    #     5) Append visualization to dashboard
    self.assertEqual(self.mock_requests_post.call_count, 5)
    self.assertEqual(self.mock_requests_get.call_count, 3)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_add_retention_diff_makes_correct_calls(self):
    self.server_calls = 0

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = self.post_server

    self.dash.add_retention_diff()

    # GET calls:
    #     1) Create dashboard
    #     2) Get dashboard widgets
    #     3) Get table ID
    # POST calls:
    #     1) Create dashboard
    #     2) Create query
    #     3) Refresh query
    #     4) Create visualization
    #     5) Append visualization to dashboard
    self.assertEqual(self.mock_requests_post.call_count, 5)
    self.assertEqual(self.mock_requests_get.call_count, 3)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_add_event_graphs_makes_correct_calls(self):
    self.server_calls = 0

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = self.post_server

    self.dash.add_event_graphs([])

    # GET calls:
    #     1) Create dashboard
    #     2) Get dashboard widgets
    #     3) Get table ID
    #     4) Repeat 3 seven times
    # POST calls:
    #     1) Create dashboard
    #     2) Create query
    #     3) Refresh query
    #     4) Create visualization
    #     5) Append visualization to dashboard
    #     6) Repeat 2-5 seven times
    self.assertEqual(self.mock_requests_post.call_count, 29)
    self.assertEqual(self.mock_requests_get.call_count, 9)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_add_ttable_makes_correct_calls(self):
    mock_boto_transfer_patcher = mock.patch("utils.transfer.upload_file")
    mock_boto_transfer_patcher.start()

    self.server_calls = 0

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = self.post_server

    self.dash.add_ttable()

    # GET calls:
    #     1) Create dashboard
    #     2) Get dashboard widgets
    #     3) Get table ID
    # POST calls:
    #     1) Create dashboard
    #     2) Create query
    #     3) Refresh query
    #     4) Append visualization to dashboard
    #     5) Get T-Table data for 20 rows
    self.assertEqual(self.mock_requests_post.call_count, 24)
    self.assertEqual(self.mock_requests_get.call_count, 3)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

    mock_boto_transfer_patcher.stop()
