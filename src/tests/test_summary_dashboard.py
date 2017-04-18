import mock
import json
import unittest

from src.redash_client import RedashClient
from src.samples.SummaryDashboard import SummaryDashboard


class TestSummaryDashboard(unittest.TestCase):

  def get_summary_dashboard(self, api_key):
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

    mock_requests_post_patcher = mock.patch("redash_client.requests.post")
    self.mock_requests_post = mock_requests_post_patcher.start()
    self.addCleanup(mock_requests_post_patcher.stop)

    mock_requests_get_patcher = mock.patch("redash_client.requests.get")
    self.mock_requests_get = mock_requests_get_patcher.start()
    self.addCleanup(mock_requests_get_patcher.stop)

    mock_requests_delete_patcher = mock.patch("redash_client.requests.delete")
    self.mock_requests_delete = mock_requests_delete_patcher.start()
    self.addCleanup(mock_requests_delete_patcher.stop)

    self.dash = self.get_summary_dashboard(API_KEY)

  def get_mock_response(self, status=200, content='{}'):
    mock_response = mock.Mock()
    mock_response.status_code = status
    mock_response.content = content

    return mock_response

  def test_update_refresh_schedule_success(self):
    EXPECTED_QUERY_ID = "query_id123"
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "visualization": {
                "query": {
                    "nope": "fail"
                }
            }}],
            [{"visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID
                }
            }},
            {"visualization": {
                "query": {
                    "muhahaha": "you can't catch me!"
                }
            }}
        ]]
    }

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))
    self.mock_requests_post.return_value = self.get_mock_response()

    self.dash.update_refresh_schedule(86400)

    # There is one get and post each in creating a dashboard
    # then 1 get for widget names and 1 post for refreshing the
    # one valid visualization iD
    self.assertEqual(self.mock_requests_post.call_count, 2)
    self.assertEqual(self.mock_requests_get.call_count, 2)
    self.assertEqual(self.mock_requests_delete.call_count, 0)
