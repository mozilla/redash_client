import mock
import json
import unittest
import requests

from src.redash_client import RedashClient
from src.constants import VizType, ChartType, VizWidth


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

    mock_requests_delete_patcher = mock.patch('redash_client.requests.delete')
    self.mock_requests_delete = mock_requests_delete_patcher.start()
    self.addCleanup(mock_requests_delete_patcher.stop)

  def get_mock_response(self, status=200, content='{}'):
    mock_response = mock.Mock()
    mock_response.status_code = status
    mock_response.content = content

    return mock_response

  def test_request_exception_thrown(self):
    ERROR_STRING = "FAIL"

    def server_call_raising_exception(url, data):
      raise requests.RequestException(ERROR_STRING)

    self.mock_requests_post.side_effect = server_call_raising_exception

    url = "www.test.com"
    self.assertRaisesRegexp(
        self.redash.RedashClientException,
        "Unable to communicate with redash: {0}".format(ERROR_STRING),
        lambda: self.redash._make_request(None, url, args={}))

  def test_failed_request_throws(self):
    STATUS = 404
    ERROR_STRING = "FAIL"
    self.mock_requests_post.return_value = self.get_mock_response(
        STATUS, ERROR_STRING)

    url = "www.test.com"
    self.assertRaisesRegexp(
        self.redash.RedashClientException,
        "Error status returned: {0} {1}".format(STATUS, ERROR_STRING),
        lambda: self.redash._make_request(None, url, args={}))

  def test_failed_to_load_content_json(self):
    BAD_JSON = "boop beep _ epic json fail"
    JSON_ERROR = "No JSON object could be decoded"
    self.mock_requests_post.return_value = self.get_mock_response(
        content=BAD_JSON)

    url = "www.test.com"
    self.assertRaisesRegexp(
        self.redash.RedashClientException,
        "Unable to parse JSON response: {0}".format(JSON_ERROR),
        lambda: self.redash._make_request(None, url, args={}))

  def test_create_new_query_returns_expected_ids(self):
    EXPECTED_QUERY_ID = "query_id123"
    EXPECTED_VIZ_ID = "viz_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }

    VISUALIZATION_LIST_RESPONSE = {
        "visualizations": [{
            "id": EXPECTED_VIZ_ID
        }]
    }

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

  def test_create_new_query_returns_none(self):
    QUERY_FAULTY_RESPONSE = {
        "some_bad_response": "boop"
    }

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_FAULTY_RESPONSE))

    query_id, table_id = self.redash.create_new_query(
        "Dash Name",
        "SELECT * FROM test", 5)

    self.assertEqual(query_id, None)
    self.assertEqual(table_id, None)
    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 0)

  def test_immediate_query_results_are_correct(self):
    EXPECTED_ROWS = [{
        "col1": 123,
        "col2": 456,
    }, {
        "col1": 789,
        "col2": 123,
    }]

    QUERY_RESULTS_RESPONSE = {
        "query_result": {
            "data": {
                "rows": EXPECTED_ROWS
            }
        }
    }

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_RESPONSE))

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertItemsEqual(rows, EXPECTED_ROWS)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_late_response_query_results_are_correct(self):
    EXPECTED_ROWS = [{
        "col1": 123,
        "col2": 456,
    }, {
        "col1": 789,
        "col2": 123,
    }]

    QUERY_RESULTS_RESPONSE = {
        "query_result": {
            "data": {
                "rows": EXPECTED_ROWS
            }
        }
    }
    QUERY_RESULTS_NOT_READY_RESPONSE = {
        "job": {}
    }

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
    QUERY_RESULTS_NOT_READY_RESPONSE = {
        "job": {}
    }

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertEqual(rows, [])
    self.assertEqual(self.mock_requests_post.call_count, 5)

  def test_new_visualization_throws_for_missing_chart_data(self):
    EXPECTED_QUERY_ID = "query_id123"

    self.assertRaises(ValueError,
                      lambda: self.redash.create_new_visualization(
                          EXPECTED_QUERY_ID, VizType.CHART))

  def test_new_visualization_throws_for_missing_cohort_data(self):
    EXPECTED_QUERY_ID = "query_id123"

    self.assertRaises(ValueError,
                      lambda: self.redash.create_new_visualization(
                          EXPECTED_QUERY_ID, VizType.COHORT))

  def test_new_visualization_throws_for_unexpected_visualization_type(self):
    EXPECTED_QUERY_ID = "query_id123"

    self.assertRaises(ValueError,
                      lambda: self.redash.create_new_visualization(
                          EXPECTED_QUERY_ID, "boop"))

  def test_new_viz_returns_expected_query_id(self):
    EXPECTED_QUERY_ID = "query_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }
    TIME_INTERVAL = "weekly"

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.create_new_visualization(
        EXPECTED_QUERY_ID, VizType.COHORT, time_interval=TIME_INTERVAL)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_format_cohort_options_correctly(self):
    TIME_INTERVAL = "weekly"
    COHORT_OPTIONS = {
        "timeInterval": TIME_INTERVAL
    }

    options = self.redash.make_visualization_options(
        viz_type=VizType.COHORT, time_interval=TIME_INTERVAL)
    self.assertItemsEqual(options, COHORT_OPTIONS)

  def test_format_chart_options_correctly(self):
    COLUMN_MAPPING = {"date": "x", "event_rate": "y", "type": "series"}
    CHART_OPTIONS = {
        "globalSeriesType": ChartType.LINE,
        "sortX": True,
        "legend": {"enabled": True},
        "yAxis": [{"type": "linear"}, {"type": "linear", "opposite": True}],
        "series": {"stacking": None},
        "xAxis": {"type": "datetime", "labels": {"enabled": True}},
        "seriesOptions": {},
        "columnMapping": COLUMN_MAPPING,
        "bottomMargin": 50
    }

    options = self.redash.make_visualization_options(
        ChartType.LINE, VizType.CHART, COLUMN_MAPPING)
    self.assertItemsEqual(options, CHART_OPTIONS)

  def test_make_correct_slug(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_SLUG = "activity-stream-a-b-testing-beep-meep"

    produced_slug = self.redash.get_slug(DASH_NAME)
    self.assertEqual(produced_slug, EXPECTED_SLUG)

  def test_new_dashboard_exists(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_QUERY_ID = "query_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.create_new_dashboard(DASH_NAME)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 0)

  def test_new_dashboard_doesnt_exist(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_QUERY_ID = "query_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }

    self.mock_requests_get.return_value = self.get_mock_response(status=404)
    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(QUERY_ID_RESPONSE))

    query_id = self.redash.create_new_dashboard(DASH_NAME)

    self.assertEqual(query_id, EXPECTED_QUERY_ID)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_publish_dashboard_success(self):
    self.mock_requests_post.return_value = self.get_mock_response()

    self.redash.publish_dashboard(dash_id=1234)

    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 0)

  def test_remove_visualization_success(self):
    self.mock_requests_delete.return_value = self.get_mock_response()

    self.redash.remove_visualization(viz_id=1234)

    self.assertEqual(self.mock_requests_post.call_count, 0)
    self.assertEqual(self.mock_requests_get.call_count, 0)
    self.assertEqual(self.mock_requests_delete.call_count, 1)

  def test_delete_query_success(self):
    self.mock_requests_delete.return_value = self.get_mock_response()

    self.redash.delete_query(query_id=1234)

    self.assertEqual(self.mock_requests_post.call_count, 0)
    self.assertEqual(self.mock_requests_get.call_count, 0)
    self.assertEqual(self.mock_requests_delete.call_count, 1)

  def test_add_visualization_to_dashboard_success(self):
    self.mock_requests_post.return_value = self.get_mock_response()

    self.redash.add_visualization_to_dashboard(
        dash_id=1234, viz_id=5678, viz_width=VizWidth.WIDE)

    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 0)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_add_visualization_to_dashboard_throws(self):
    self.assertRaises(ValueError,
                      lambda: self.redash.add_visualization_to_dashboard(
                          dash_id=1234, viz_id=5678, viz_width="meep"))

  def test_update_query_schedule_success(self):
    self.mock_requests_post.return_value = self.get_mock_response()

    self.redash.update_query_schedule(query_id=1234, schedule=86400)

    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 0)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_get_widget_from_dash_returns_correctly_flattened_widgets(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_QUERY_ID = "query_id123"
    EXPECTED_QUERY_ID2 = "query_id456"
    EXPECTED_QUERY_ID3 = "query_id789"
    FLAT_WIDGETS = [{
        "visualization": {
            "query": {
                "id": EXPECTED_QUERY_ID
            }
        }
    }, {
        "visualization": {
            "query": {
                "id": EXPECTED_QUERY_ID2
            }
        }
    }, {
        "visualization": {
            "query": {
                "id": EXPECTED_QUERY_ID3
            }
        }
    }]

    WIDGETS_RESPONSE = {
        "widgets": [[{
            "visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID
                }
            }}],
            [{"visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID2
                }
            }},
            {"visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID3
                }
            }}
        ]]
    }

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))

    widget_list = self.redash.get_widget_from_dash(DASH_NAME)

    self.assertEqual(widget_list, FLAT_WIDGETS)
    self.assertEqual(self.mock_requests_get.call_count, 1)


if __name__ == '__main__':
  unittest.main()
