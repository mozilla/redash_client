import mock
import json

from src.tests.base import AppTest
from constants import TTableSchema
from utils import upload_as_json, download_experiment_definition


class TestUtils(AppTest):

  def test_upload_as_json_return_val(self):
    DIRECTORY_NAME = "experiments"
    FILENAME = "test_file_name"
    DATA = {"columns": TTableSchema, "rows": []}

    EXPECTED_S3_KEY = "activity-stream/" + DIRECTORY_NAME + "/" + FILENAME
    EXPECTED_BASE_URL = "https://analysis-output.telemetry.mozilla.org/"

    mock_boto_transfer_patcher = mock.patch("utils.transfer.upload_file")
    mock_boto_transfer_patcher.start()

    query_string = upload_as_json(DIRECTORY_NAME, FILENAME, DATA)

    self.assertEqual(query_string, EXPECTED_BASE_URL + EXPECTED_S3_KEY)

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_non_json_return_val(self):
    mock_boto_transfer_patcher = mock.patch("utils.transfer.download_file")
    mock_transfer = mock_boto_transfer_patcher.start()
    mock_transfer.return_value = "fail"

    json_result = download_experiment_definition()

    self.assertEqual(json_result, {})

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_json_return_val(self):
    EXPECTED_JSON = json.dumps({"experiment1": "some_value"})

    mock_boto_download_patcher = mock.patch("utils.transfer.download_file")
    mock_download = mock_boto_download_patcher.start()
    mock_download.return_value = EXPECTED_JSON

    json_result = download_experiment_definition()

    self.assertEqual(json_result, json.loads(EXPECTED_JSON))

    mock_boto_download_patcher.stop()
