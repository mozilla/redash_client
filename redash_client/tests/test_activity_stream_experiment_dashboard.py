import json
import time

from redash_client.tests.base import AppTest
from redash_client.dashboards.ActivityStreamExperimentDashboard import (
    ActivityStreamExperimentDashboard)


class TestActivityStreamExperimentDashboard(AppTest):

  START_DATE = "2017-17-02"
  END_DATE = time.strftime("%Y-%m-%d")
  DASH_PROJECT = "Activity Stream Experiment"
  DASH_NAME = "Screenshots Long Cache"
  EXPERIMENT_ID = "exp-014-screenshotsasync"

  def get_dashboard(self, api_key):
    self.mock_requests_get.return_value = self.get_mock_response()
    self.mock_requests_post.return_value = self.get_mock_response()

    dashboard = ActivityStreamExperimentDashboard(
        self.redash,
        self.DASH_PROJECT,
        self.DASH_NAME,
        self.EXPERIMENT_ID,
        self.START_DATE,
    )
    return dashboard

  def test_correct_values_at_initialization(self):
    self.assertEqual(self.dash._experiment_id, self.EXPERIMENT_ID)
    self.assertEqual(
        self.dash._dash_name,
        "{project}: {dash}".format(
            project=self.DASH_PROJECT, dash=self.DASH_NAME))
    self.assertEqual(self.dash._start_date, self.START_DATE)
    self.assertEqual(self.dash._end_date, self.END_DATE)

    # 2 posts to create the dashboard and make it public
    self.assertEqual(self.mock_requests_post.call_count, 2)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_delete.call_count, 0)

  def test_add_templates_makes_correct_calls(self):
    self.get_calls = 0
    QUERIES_IN_SEARCH = [{
        "id": 5,
        "description": "SomeQuery",
        "name": "AS Template: Query Title Event",
        "data_source_id": 5
    }, {
        "id": 6,
        "description": "SomeQuery2",
        "name": "AS Template: Query Title",
        "data_source_id": 5
    }]
    VISUALIZATIONS_FOR_QUERY = {
        "visualizations": [
            {"options": {}},
            {"options": {}}
        ]
    }
    WIDGETS_RESPONSE = {
        "widgets": [[{
            "id": "the_widget_id",
            "visualization": {
                "query": {
                    "id": "some_id",
                    "name": "Query Title Click"
                },
            },
        }]]
    }

    def get_server(url):
      response = self.get_mock_response()
      if self.get_calls == 0:
        response = self.get_mock_response(
            content=json.dumps(QUERIES_IN_SEARCH))
      elif self.get_calls <= 2:
        response = self.get_mock_response(
            content=json.dumps(VISUALIZATIONS_FOR_QUERY))
      else:
        response = self.get_mock_response(
            content=json.dumps(WIDGETS_RESPONSE))

      self.get_calls += 1
      return response

    self.server_calls = 0
    self.mock_requests_delete.return_value = self.get_mock_response()
    self.mock_requests_post.side_effect = self.post_server
    self.mock_requests_get.side_effect = get_server

    self.dash.add_graph_templates("Template:")

    # GET calls:
    #     1) Create dashboard
    #     2) Get dashboard widgets
    #     3) Search queries
    #     4) Get two existing visualizations
    # POST calls:
    #     1) Create dashboard
    #     2) Search queries
    #     3) Fork query
    #     4) Update query
    #     5) Create visualization
    #     6) Append visualization to dashboard
    #     7) Repeat 2-6 six times
    #     8) Make dashboard public
    # DELETE calls:
    #     One existing graph is removed from dashboard
    #     and deleted (2 calls)
    self.assertEqual(self.mock_requests_post.call_count, 32)
    self.assertEqual(self.mock_requests_get.call_count, 5)
    self.assertEqual(self.mock_requests_delete.call_count, 2)
