import mock

from utils import upload_as_json
from src.tests.base import AppTest
from constants import TTableSchema


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