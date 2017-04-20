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
    self.assertEqual(self.dash.addon_versions, "'1.8.0', '1.9.0'")

    self.assertEqual(self.mock_requests_post.call_count, 1)
    self.assertEqual(self.mock_requests_get.call_count, 1)
    self.assertEqual(self.mock_requests_delete.call_count, 0)
