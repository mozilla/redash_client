import json

from src.constants import RetentionType
from src.tests.base import AppTest
from templates import active_users


class TestSummaryDashboard(AppTest):

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

  def test_get_chart_names_success(self):
    EXPECTED_QUERY_NAME = "query_name123"
    EXPECTED_QUERY_NAME2 = "query_name456"
    EXPECTED_QUERY_NAME3 = "query_name789"
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "visualization": {
                "query": {
                    "name": EXPECTED_QUERY_NAME
                }
            }}],
            [{"visualization": {
                "query": {
                    "not_a_name": EXPECTED_QUERY_NAME2
                }
            }},
            {"visualization": {
                "query": {
                    "name": EXPECTED_QUERY_NAME3
                }
            }}
        ]]
    }
    EXPECTED_SET = set([EXPECTED_QUERY_NAME,
                        EXPECTED_QUERY_NAME3])

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))

    chart_names = self.dash.get_chart_names()

    self.assertEqual(chart_names, EXPECTED_SET)

  def test_remove_all_graphs_success(self):
    EXPECTED_QUERY_ID = "query_id123"
    EXPECTED_QUERY_ID2 = "query_id456"
    EXPECTED_QUERY_ID3 = "query_id789"
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "id": EXPECTED_QUERY_ID,
            "visualization": {
                "query": {
                    "id": EXPECTED_QUERY_ID
                }
            }}], [{
                "id": EXPECTED_QUERY_ID2,
                "visualization": {
                    "query": {
                        "id": EXPECTED_QUERY_ID2
                    }
                }
            }, {
                "id": EXPECTED_QUERY_ID3,
                "visualization": {
                    "query": {
                        "id": EXPECTED_QUERY_ID3
                    }
                }
            }
        ]]
    }

    self.mock_requests_delete.return_value = self.get_mock_response()
    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))

    self.dash.remove_all_graphs()

    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 2)
    self.assertEqual(self.mock_requests_delete.call_count, 6)

  def test_mau_dau_column_mapping_returns_correct_mappings(self):
    EXPECTED_MAU_DAU_MAPPING = {
        "date": "x",
        "dau": "y",
        "wau": "y",
        "mau": "y",
    }
    EXPECTED_ENGAGEMENT_RATIO_MAPPING = {
        "date": "x",
        "weekly_engagement": "y",
        "monthly_engagement": "y",
    }

    query_string, fields = active_users(
        self.dash._events_table, self.dash._start_date)
    mau_mapping, er_mapping = self.dash._get_mau_dau_column_mappings(fields)

    self.assertEqual(mau_mapping, EXPECTED_MAU_DAU_MAPPING)
    self.assertEqual(er_mapping, EXPECTED_ENGAGEMENT_RATIO_MAPPING)

  def test_mau_dau_graphs_exist_makes_no_request(self):
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "visualization": {
                "query": {
                    "name": self.dash.MAU_DAU_TITLE,
                },
            },
        }]]
    }

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))

    self.dash.add_mau_dau()

    # Only 1 each for post and get to set up the dashboard
    # Then one get for looking up chart names
    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 2)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_mau_dau_graphs_make_expected_calls(self):
    EXPECTED_QUERY_ID = "query_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }

    self.server_calls = 0

    def post_server(url, data):
      response = self.get_mock_response()
      if self.server_calls % 2 == 0 or self.server_calls == 0:
        response = self.get_mock_response(
            content=json.dumps(QUERY_ID_RESPONSE))

      self.server_calls += 1
      return response

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = post_server

    self.dash.add_mau_dau()

    # GET calls:
    #     1) Create dashboard
    #     2) Get dashboard widgets
    #     3) Get first table ID
    #     4) Get second table ID
    # POST calls:
    #     1) Create dashboard
    #     2) Create first query
    #     3) Refresh first query
    #     4) Create second query
    #     5) Refresh second query
    #     6) Create first visualization
    #     7) Append first visualization to dashboard
    #     8) Create second visualization
    #     9) Append second visualization to dashboard
    self.assertEqual(self.mock_requests_post.call_count, 9)
    self.assertEqual(self.mock_requests_get.call_count, 4)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_retention_graphs_exist_makes_no_request(self):
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "visualization": {
                "query": {
                    "name": self.dash.WEEKLY_RETENTION_TITLE,
                },
            },
        }]]
    }

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))

    self.dash.add_retention_graph(RetentionType.WEEKLY)

    # Only 1 each for post and get to set up the dashboard
    # Then one get for looking up chart names
    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 2)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_retention_graph_makes_expected_calls(self):
    EXPECTED_QUERY_ID = "query_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }

    self.server_calls = 0

    def post_server(url, data):
      response = self.get_mock_response()
      if self.server_calls % 2 == 0 or self.server_calls == 0:
        response = self.get_mock_response(
            content=json.dumps(QUERY_ID_RESPONSE))

      self.server_calls += 1
      return response

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = post_server

    self.dash.add_retention_graph(RetentionType.DAILY)

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

  def test_weekly_events_graph_exist_makes_no_request(self):
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "visualization": {
                "query": {
                    "name": self.dash.EVENTS_WEEKLY_TITLE,
                },
            },
        }]]
    }

    self.mock_requests_get.return_value = self.get_mock_response(
        content=json.dumps(WIDGETS_RESPONSE))

    self.dash.add_events_weekly()

    # Only 1 each for post and get to set up the dashboard
    # Then one get for looking up chart names
    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 2)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_weekly_events_graph_makes_expected_calls(self):
    EXPECTED_QUERY_ID = "query_id123"
    QUERY_ID_RESPONSE = {
        "id": EXPECTED_QUERY_ID
    }

    self.server_calls = 0

    def post_server(url, data):
      response = self.get_mock_response()
      if self.server_calls % 2 == 0 or self.server_calls == 0:
        response = self.get_mock_response(
            content=json.dumps(QUERY_ID_RESPONSE))

      self.server_calls += 1
      return response

    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = post_server

    self.dash.add_events_weekly()

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
