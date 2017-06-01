import mock
import json
import tempfile

from src.tests.base import AppTest
from src.constants import TTableSchema
from src.utils import upload_as_json, read_experiment_definition


class TestUtils(AppTest):

  def test_upload_as_json_return_val(self):
    DIRECTORY_NAME = "experiments"
    FILENAME = "test_file_name"
    DATA = {"columns": TTableSchema, "rows": []}

    EXPECTED_S3_KEY = "activity-stream/" + DIRECTORY_NAME + "/" + FILENAME
    EXPECTED_BASE_URL = "https://analysis-output.telemetry.mozilla.org/"

    mock_boto_transfer_patcher = mock.patch("src.utils.transfer.upload_file")
    mock_boto_transfer_patcher.start()

    query_string = upload_as_json(DIRECTORY_NAME, FILENAME, DATA)

    self.assertEqual(query_string, EXPECTED_BASE_URL + EXPECTED_S3_KEY)

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_non_json_return_val(self):
    mock_boto_transfer_patcher = mock.patch("src.utils.s3.get_object")
    mock_transfer = mock_boto_transfer_patcher.start()
    mock_transfer.return_value = "fail"

    json_result = read_experiment_definition("beep")

    self.assertEqual(json_result, {})

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_json_return_val(self):
    EXPECTED_JSON = json.dumps({"experiment1": "some_value"})

    mock_boto_download_patcher = mock.patch("src.utils.s3.get_object")
    mock_download = mock_boto_download_patcher.start()

    # Make a temp file for returning
    temp_file = tempfile.mkstemp()
    file_handle = open(temp_file[1], "w+")
    file_handle.write(EXPECTED_JSON)
    file_handle.seek(0)

    mock_download.return_value = {"Body": file_handle}

    json_result = read_experiment_definition("boop")

    self.assertEqual(json_result, json.loads(EXPECTED_JSON))

    mock_boto_download_patcher.stop()
    file_handle.close()
