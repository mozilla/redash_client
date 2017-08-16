import mock
import json
import unittest

from redash_client.client import RedashClient
from redash_client.dashboards.SummaryDashboard import SummaryDashboard


class AppTest(unittest.TestCase):

  def post_server(self, url, data):
    EXPECTED_QUERY_ID = "query_id123"
    EXPECTED_QUERY_STRING = "select some_stuff from table"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }
    FORK_RESPONSE = {
        "id": EXPECTED_QUERY_ID,
        "query": EXPECTED_QUERY_STRING,
        "data_source_id": 3
    }

    response = self.get_mock_response()
    if self.server_calls % 2 == 0 or self.server_calls == 0:
      response = self.get_mock_response(
          content=json.dumps(QUERY_ID_RESPONSE))

    if "fork" in url:
      response = self.get_mock_response(
          content=json.dumps(FORK_RESPONSE))

    self.server_calls += 1
    return response

  def get_dashboard(self, api_key):
    EVENTS_TABLE_NAME = "activity_stream_mobile_events_daily"
    START_DATE = "02/17/2017"
    DASH_NAME = "Firefox iOS: Metrics Summary"

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.return_value = self.get_mock_response()

    dashboard = SummaryDashboard(
        self.redash,
        DASH_NAME,
        EVENTS_TABLE_NAME,
        START_DATE,
    )
    return dashboard

  def setUp(self):
    API_KEY = "test_key"

    self.redash = RedashClient(API_KEY)

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

    self.dash = self.get_dashboard(API_KEY)

  def get_mock_response(self, status=200, content='{}'):
    mock_response = mock.Mock()
    mock_response.status_code = status
    mock_response.content = content

    return mock_response
