import mock
import json
import unittest

from testdata import *
from redash_client import RedashClient
from constants import VizType, ChartType

class TestRedashClient(unittest.TestCase):
  def setUp(self):
    api_key = "test_key"
    self.redash = RedashClient(api_key)

    mock_requests_post_patcher = mock.patch('redash_client.requests.post')
    self.mock_requests_post = mock_requests_post_patcher.start()
    self.addCleanup(mock_requests_post_patcher.stop)

    mock_requests_get_patcher = mock.patch('redash_client.requests.get')
    self.mock_requests_get = mock_requests_get_patcher.start()
    self.addCleanup(mock_requests_get_patcher.stop)

  def get_mock_response(self, status=200, content='{}'):
    def json_function():
      return json.loads(content)

    mock_response = mock.Mock()
    mock_response.status_code = status
    mock_response.content = content
    mock_response.json = json_function

    return mock_response

  def test_create_new_query_returns_expected_ids(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))
    self.mock_requests_get.return_value = self.get_mock_response(
      content=json.dumps(VISUALIZATION_LIST_RESPONSE))

    query_id, table_id = self.redash.create_new_query(
      "Dash Name",
      "SELECT * FROM test", 5)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(table_id, EXPECTED_VIZ_ID)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 2)

  def test_immediate_query_results_are_correct(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_RESULTS_RESPONSE))

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertItemsEqual(rows, EXPECTED_ROWS)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_late_response_query_results_are_correct(self):
    self.server_calls = 0

    def simulate_server_calls(url, data):
      response = QUERY_RESULTS_NOT_READY_RESPONSE
      if self.server_calls >= 2:
        response = QUERY_RESULTS_RESPONSE

      self.server_calls += 1
      return self.get_mock_response(content=json.dumps(response))

    self.mock_requests_post.side_effect = simulate_server_calls

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertEqual(rows, EXPECTED_ROWS)
    self.assertEqual(self.mock_requests_post.call_count, 3)

  def test_query_results_not_available(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertEqual(rows, [])
    self.assertEqual(self.mock_requests_post.call_count, 5)

  def test_new_visualization_throws_for_missing_chart_data(self):
    self.assertRaises(ValueError,
      lambda: self.redash.create_new_visualization(EXPECTED_QUERY_ID, VizType.CHART))

  def test_new_visualization_throws_for_missing_cohort_data(self):
    self.assertRaises(ValueError,
      lambda: self.redash.create_new_visualization(EXPECTED_QUERY_ID, VizType.COHORT))

  def test_new_visualization_throws_for_unexpected_visualization_type(self):
    self.assertRaises(ValueError,
      lambda: self.redash.create_new_visualization(EXPECTED_QUERY_ID, "boop"))

  def test_new_viz_returns_expected_query_id(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.create_new_visualization(
      EXPECTED_QUERY_ID, VizType.COHORT, time_interval=TIME_INTERVAL)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_format_cohort_options_correctly(self):
    options = self.redash.make_visualization_options(
      viz_type=VizType.COHORT, time_interval=TIME_INTERVAL)
    self.assertItemsEqual(options, COHORT_OPTIONS)

  def test_format_chart_options_correctly(self):
    options = self.redash.make_visualization_options(
      ChartType.LINE, VizType.CHART, COLUMN_MAPPING)
    self.assertItemsEqual(options, CHART_OPTIONS)

  def test_make_correct_slug(self):
    produced_slug = self.redash.get_slug(DASH_NAME)
    self.assertEqual(produced_slug, EXPECTED_SLUG)

  def test_new_dashboard_exists(self):
    self.mock_requests_get.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.create_new_dashboard(DASH_NAME)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 0)

  def test_new_dashboard_doesnt_exist(self):
    self.mock_requests_get.return_value = self.get_mock_response(status=404)
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.create_new_dashboard(DASH_NAME)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_get_widget_from_dash_returns_correctly_flattened_widgets(self):
    self.mock_requests_get.return_value = self.get_mock_response(
      content=json.dumps(WIDGETS_RESPONSE))

    widget_list = self.redash.get_widget_from_dash(DASH_NAME)

    self.assertEqual(widget_list, FLAT_WIDGETS)
    self.assertEqual(self.mock_requests_get.call_count, 1)

if __name__ == '__main__':
  unittest.main()
