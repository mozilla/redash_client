import mock
import json
import requests

from redash_client.tests.base import AppTest
from redash_client.client import RedashClient
from redash_client.constants import VizType, ChartType, VizWidth


class TestRedashClient(AppTest):

  def setUp(self):
    # Maintain python2 compatibility
    if not hasattr(self, 'assertCountEqual'):
      self.assertCountEqual = self.assertItemsEqual
    if not hasattr(self, 'assertRaisesRegex'):
      self.assertRaisesRegex = self.assertRaisesRegexp

    api_key = "test_key"
    self.redash = RedashClient(api_key)

    mock_requests_post_patcher = mock.patch(
        "redash_client.client.requests.post")
    self.mock_requests_post = mock_requests_post_patcher.start()
    self.addCleanup(mock_requests_post_patcher.stop)

    mock_requests_get_patcher = mock.patch(
        "redash_client.client.requests.get")
    self.mock_requests_get = mock_requests_get_patcher.start()
    self.addCleanup(mock_requests_get_patcher.stop)

    mock_requests_delete_patcher = mock.patch(
        "redash_client.client.requests.delete")
    self.mock_requests_delete = mock_requests_delete_patcher.start()
    self.addCleanup(mock_requests_delete_patcher.stop)

  def test_request_exception_thrown(self):
    ERROR_STRING = "FAIL"

    def server_call_raising_exception(url, data):
      raise requests.RequestException(ERROR_STRING)

    self.mock_requests_post.side_effect = server_call_raising_exception

    url = "www.test.com"
    self.assertRaisesRegex(
        self.redash.RedashClientException,
        "Unable to communicate with redash: {0}".format(ERROR_STRING),
        lambda: self.redash._make_request(None, url, req_args={}))

  def test_failed_request_throws(self):
    STATUS = 404
    ERROR_STRING = "FAIL"
    self.mock_requests_post.return_value = self.get_mock_response(
        STATUS, ERROR_STRING)

    url = "www.test.com"
    self.assertRaisesRegex(
        self.redash.RedashClientException,
        "Error status returned: {0} {1}".format(STATUS, ERROR_STRING),
        lambda: self.redash._make_request(None, url, req_args={}))

  def test_failed_to_load_content_json(self):
    BAD_JSON = "boop beep _ epic json fail"
    JSON_ERROR = "No JSON object could be decoded"
    post_response = self.get_mock_response(content=BAD_JSON)
    post_response.json.side_effect = ValueError(JSON_ERROR)
    self.mock_requests_post.return_value = post_response

    url = "www.test.com"
    self.assertRaisesRegex(
        self.redash.RedashClientException,
        "Unable to parse JSON response: {0}".format(JSON_ERROR),
        lambda: self.redash._make_request(None, url, req_args={}))

  def test_get_public_url_returns_expected_url(self):
    DASH_ID = 6
    EXPECTED_PUBLIC_URL = {"public_url": "www.example.com/expected"}
    post_response = self.get_mock_response(
        content=json.dumps(EXPECTED_PUBLIC_URL))
    post_response.json.return_value = EXPECTED_PUBLIC_URL
    self.mock_requests_post.return_value = post_response

    public_url = self.redash.get_public_url(DASH_ID)
    self.assertEqual(public_url, EXPECTED_PUBLIC_URL["public_url"])

  def test_get_visualization_public_url_has_correct_url(self):
    WIDGET_ID = 123
    QUERY_ID = 456
    URL_PARAM = "api_key={api_key}".format(api_key=self.redash._api_key)

    EXPECTED_PUBLIC_URL = ("https://sql.telemetry.mozilla.org/embed/"
                           "query/{query_id}/visualization/{viz_id}"
                           "?{url_param}").format(
        query_id=QUERY_ID, viz_id=WIDGET_ID, url_param=URL_PARAM)

    public_url = self.redash.get_visualization_public_url(QUERY_ID, WIDGET_ID)
    self.assertEqual(public_url, EXPECTED_PUBLIC_URL)

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

    query_id_response_json = json.dumps(QUERY_ID_RESPONSE)
    post_response = self.get_mock_response(content=query_id_response_json)
    post_response.json.return_value = QUERY_ID_RESPONSE
    self.mock_requests_post.return_value = post_response

    viz_list_response_json = json.dumps(VISUALIZATION_LIST_RESPONSE)
    get_response = self.get_mock_response(content=viz_list_response_json)
    get_response.json.return_value = VISUALIZATION_LIST_RESPONSE
    self.mock_requests_get.return_value = get_response

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
    post_response = self.get_mock_response(
        content=json.dumps(QUERY_FAULTY_RESPONSE))
    post_response.json.return_value = QUERY_FAULTY_RESPONSE
    self.mock_requests_post.return_value = post_response

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

    post_response = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_RESPONSE))
    post_response.json.return_value = QUERY_RESULTS_RESPONSE
    self.mock_requests_post.return_value = post_response

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertCountEqual(rows, EXPECTED_ROWS)
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
        "job": {"status": 1, "id": "123"}
    }

    QUERY_RESULTS_READY_RESPONSE = {
        "job": {"status": 3, "id": "123", "query_result_id": 456}
    }

    # We should have one POST request and two GET requests
    post_response = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))
    post_response.json.return_value = QUERY_RESULTS_NOT_READY_RESPONSE
    self.mock_requests_post.return_value = post_response

    self.get_calls = 0

    def simulate_get_calls(url):
      if self.get_calls == 0:
        self.assertTrue("jobs" in url)
        self.assertTrue("123" in url)
        response = QUERY_RESULTS_READY_RESPONSE
        self.get_calls += 1
      else:
        self.assertTrue("query_results" in url)
        self.assertTrue("456" in url)
        response = QUERY_RESULTS_RESPONSE

      get_response = self.get_mock_response(content=json.dumps(response))
      get_response.json.return_value = response
      return get_response

    self.mock_requests_get.side_effect = simulate_get_calls

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertEqual(rows, EXPECTED_ROWS)
    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 2)

  def test_query_results_not_available(self):
    QUERY_RESULTS_NOT_READY_RESPONSE = {
        "job": {"status": 1, "id": "123"}
    }

    self.redash._retry_delay = .000000001

    post_response = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))
    post_response.json.return_value = QUERY_RESULTS_NOT_READY_RESPONSE
    self.mock_requests_post.return_value = post_response

    get_response = self.get_mock_response(
        content=json.dumps(QUERY_RESULTS_NOT_READY_RESPONSE))
    get_response.json.return_value = QUERY_RESULTS_NOT_READY_RESPONSE
    self.mock_requests_get.return_value = get_response

    rows = self.redash.get_query_results("SELECT * FROM test", 5)

    self.assertEqual(rows, [])
    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 5)

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

    post_response = self.get_mock_response(
        content=json.dumps(QUERY_ID_RESPONSE))
    post_response.json.return_value = QUERY_ID_RESPONSE
    self.mock_requests_post.return_value = post_response

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
    self.assertCountEqual(options, COHORT_OPTIONS)

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
    self.assertCountEqual(options, CHART_OPTIONS)

  def test_make_correct_slug(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_SLUG = "activity-stream-a-b-testing-beep-meep"

    produced_slug = self.redash.get_slug(DASH_NAME)
    self.assertEqual(produced_slug, EXPECTED_SLUG)

  def test_new_dashboard_exists(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_QUERY_ID = "query_id123"
    EXPECTED_SLUG = "some_slug_it_made"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID,
        "slug": EXPECTED_SLUG
    }

    get_response = self.get_mock_response(
        content=json.dumps(QUERY_ID_RESPONSE))
    get_response.json.return_value = QUERY_ID_RESPONSE
    self.mock_requests_get.return_value = get_response

    dash_info = self.redash.create_new_dashboard(DASH_NAME)

    self.assertEqual(dash_info["dashboard_id"], EXPECTED_QUERY_ID)
    self.assertEqual(dash_info["dashboard_slug"], EXPECTED_SLUG)
    self.assertEqual(
        dash_info["slug_url"],
        self.redash.BASE_URL + "dashboard/{slug}".format(slug=EXPECTED_SLUG))
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_post.call_count, 0)

  def test_new_dashboard_doesnt_exist(self):
    DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
    EXPECTED_QUERY_ID = "query_id123"
    EXPECTED_SLUG = "some_slug_it_made"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID,
        "slug": EXPECTED_SLUG
    }

    self.mock_requests_get.return_value = self.get_mock_response(status=404)
    post_response = self.get_mock_response(
        content=json.dumps(QUERY_ID_RESPONSE))
    post_response.json.return_value = QUERY_ID_RESPONSE
    self.mock_requests_post.return_value = post_response

    dash_info = self.redash.create_new_dashboard(DASH_NAME)

    self.assertEqual(dash_info["dashboard_id"], EXPECTED_QUERY_ID)
    self.assertEqual(dash_info["dashboard_slug"], EXPECTED_SLUG)
    self.assertEqual(
        dash_info["slug_url"],
        self.redash.BASE_URL + "dashboard/{slug}".format(slug=EXPECTED_SLUG))
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

  def test_update_query_string_success(self):
    self.mock_requests_post.return_value = self.get_mock_response()

    self.redash.update_query(
        query_id=1234,
        name="Test",
        sql_query="SELECT * FROM table",
        data_source_id=5,
        description="",
        options={"some_options": "an_option"}
    )

    # One call to update query, one call to refresh it
    self.assertEqual(self.mock_requests_post.call_count, 2)
    self.assertEqual(self.mock_requests_get.call_count, 0)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_fork_query_returns_correct_attributes(self):
    FORKED_QUERY = {
        "id": 5,
        "query": "sql query text",
        "data_source_id": 5
    }

    self.mock_requests_post.return_value = self.get_mock_response(
        content=json.dumps(FORKED_QUERY))

    fork = self.redash.fork_query(5)

    self.assertEqual(len(fork), 3)
    self.assertTrue("id" in fork)
    self.assertTrue("query" in fork)
    self.assertTrue("data_source_id" in fork)
    self.assertEqual(self.mock_requests_post.call_count, 1)

  def test_search_queries_returns_correct_attributes(self):
    self.get_calls = 0
    QUERIES_IN_SEARCH = {
        "results": [{
            "id": 5,
            "description": "SomeQuery",
            "name": "Query Title",
            "data_source_id": 5
        }]
    }
    VISUALIZATIONS_FOR_QUERY = {
        "visualizations": [
            {"options": {}},
            {"options": {}}
        ]
    }

    def get_server(url):
      response = self.get_mock_response()
      response.json.return_value = {}
      if self.get_calls == 0:
        response = self.get_mock_response(
            content=json.dumps(QUERIES_IN_SEARCH))
        response.json.return_value = QUERIES_IN_SEARCH
      else:
        response = self.get_mock_response(
            content=json.dumps(VISUALIZATIONS_FOR_QUERY))
        response.json.return_value = VISUALIZATIONS_FOR_QUERY

      self.get_calls += 1
      return response

    self.mock_requests_get.side_effect = get_server

    templates = self.redash.search_queries("Keyword")

    self.assertEqual(len(templates), 1)
    self.assertTrue("id" in templates[0])
    self.assertTrue("description" in templates[0])
    self.assertTrue("name" in templates[0])
    self.assertTrue("data_source_id" in templates[0])
    self.assertEqual(self.mock_requests_get.call_count, 2)

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
        "widgets": [{
            "visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID
                }
            }},
            {"visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID2
                }
            }},
            {"visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID3
                }
            }}
        ]
    }

    get_response = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))
    get_response.json.return_value = WIDGETS_RESPONSE
    self.mock_requests_get.return_value = get_response

    widget_list = self.redash.get_widget_from_dash(DASH_NAME)

    self.assertEqual(widget_list, FLAT_WIDGETS)
    self.assertEqual(self.mock_requests_get.call_count, 1)
