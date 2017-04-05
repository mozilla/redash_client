import mock
import json
import unittest
from testdata import *
from custom_timer import CustomTimer
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

  def test_init(self):
    self.assertEqual(self.redash.BASE_URL, "https://sql.telemetry.mozilla.org/api")

  def test_new_query(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))
    self.mock_requests_get.return_value = self.get_mock_response(
      content=json.dumps(VISUALIZATION_LIST_RESPONSE))

    query_id, table_id = self.redash.new_query(
      "Dash Name",
      "SELECT * FROM test", 5)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(table_id, EXPECTED_VIZ_ID)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 2)

  def test_query_results_ready_immediate(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_RESULTS_RESPONSE))

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertItemsEqual(rows, EXPECTED_ROWS)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_query_results_ready_late(self):
    self.assertEqual(self.redash._retry_count, self.redash.MAX_RETRY_COUNT)

    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))

    t = CustomTimer(0, self.redash.get_query_results, ["SELECT * FROM test", 5])
    t.start()

    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_RESULTS_RESPONSE))

    rows = t.join()

    self.assertEqual(rows, EXPECTED_ROWS)

  def test_query_results_not_ready(self):
    self.assertEqual(self.redash._retry_count, self.redash.MAX_RETRY_COUNT)

    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))

    t = CustomTimer(0, self.redash.get_query_results, ["SELECT * FROM test", 5])
    t.start()
    rows = t.join()

    self.assertEqual(rows, [])
    self.assertEqual(self.mock_requests_post.call_count, 6)

  def test_new_viz_throws(self):
    self.assertRaises(ValueError,
      lambda: self.redash.new_visualization(EXPECTED_QUERY_ID, VizType.CHART))
    self.assertRaises(ValueError,
      lambda: self.redash.new_visualization(EXPECTED_QUERY_ID, VizType.COHORT))
    self.assertRaises(ValueError,
      lambda: self.redash.new_visualization(EXPECTED_QUERY_ID, "boop"))

  def test_new_viz(self):
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.new_visualization(EXPECTED_QUERY_ID, VizType.COHORT, time_interval=TIME_INTERVAL)
    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_make_viz_options(self):
    options = self.redash.make_viz_options(viz_type=VizType.COHORT, time_interval=TIME_INTERVAL)
    self.assertItemsEqual(options, COHORT_OPTIONS)

    options = self.redash.make_viz_options(ChartType.LINE, VizType.CHART, COLUMN_MAPPING)
    self.assertItemsEqual(options, CHART_OPTIONS)

  def test_slug(self):
    produced_slug = self.redash.get_slug(DASH_NAME)
    self.assertEqual(produced_slug, EXPECTED_SLUG)

  def test_new_dashboard_exists(self):
    self.mock_requests_get.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))
    query_id = self.redash.new_dashboard(DASH_NAME)
    self.assertEqual(query_id, EXPECTED_QUERY_ID)

    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 0)

  def test_new_dashboard_doesnt_exist(self):
    self.mock_requests_get.return_value = self.get_mock_response(404)
    self.mock_requests_post.return_value = self.get_mock_response(
      content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.new_dashboard(DASH_NAME)
    self.assertEqual(query_id, EXPECTED_QUERY_ID)

    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_get_widget_from_dash(self):
    self.mock_requests_get.return_value = self.get_mock_response(
      content=json.dumps(WIDGETS_RESPONSE))
    widget_list = self.redash.get_widget_from_dash(DASH_NAME)

    self.assertEqual(widget_list, FLAT_WIDGETS)
    self.assertEqual(self.mock_requests_get.call_count, 1)

if __name__ == '__main__':
  unittest.main()
