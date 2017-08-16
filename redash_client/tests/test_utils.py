import mock
import json
import tempfile
import calendar
from datetime import datetime, timedelta

from redash_client.tests.base import AppTest
from redash_client.constants import TTableSchema
from redash_client.utils import (
    upload_as_json, read_experiment_definition,
    read_experiment_definition_s3, format_date, is_old_date)


class TestUtils(AppTest):

  def test_upload_as_json_return_val(self):
    DIRECTORY_NAME = "experiments"
    FILENAME = "test_file_name"
    DATA = {"columns": TTableSchema, "rows": []}

    EXPECTED_S3_KEY = "activity-stream/" + DIRECTORY_NAME + "/" + FILENAME
    EXPECTED_BASE_URL = "https://analysis-output.telemetry.mozilla.org/"

    mock_boto_transfer_patcher = mock.patch("redash_client.utils.transfer.upload_file")
    mock_boto_transfer_patcher.start()

    query_string = upload_as_json(DIRECTORY_NAME, FILENAME, DATA)

    self.assertEqual(query_string, EXPECTED_BASE_URL + EXPECTED_S3_KEY)

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_json_non_json_return_val(self):
    mock_boto_transfer_patcher = mock.patch("redash_client.utils.s3.get_object")
    mock_transfer = mock_boto_transfer_patcher.start()
    mock_transfer.return_value = "fail"

    json_result = read_experiment_definition_s3("beep")

    self.assertEqual(json_result, {})

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_non_json_return_val(self):
    mock_boto_transfer_patcher = mock.patch("redash_client.utils.urllib.urlopen")
    mock_transfer = mock_boto_transfer_patcher.start()
    mock_transfer.return_value = "fail"

    json_result = read_experiment_definition("beep")

    self.assertEqual(json_result, {})

    mock_boto_transfer_patcher.stop()

  def test_download_experiment_definition_json_return_val(self):
    EXPECTED_JSON = json.dumps({"experiment1": "some_value"})

    download_patcher = mock.patch("redash_client.utils.urllib.urlopen")
    mock_download = download_patcher.start()

    # Make a temp file for returning
    temp_file = tempfile.mkstemp()
    file_handle = open(temp_file[1], "w+")
    file_handle.write(EXPECTED_JSON)
    file_handle.seek(0)

    mock_download.return_value = file_handle

    json_result = read_experiment_definition("boop")

    self.assertEqual(json_result, json.loads(EXPECTED_JSON))

    download_patcher.stop()
    file_handle.close()

  def test_download_experiment_definition_s3_json_return_val(self):
    EXPECTED_JSON = json.dumps({"experiment1": "some_value"})

    mock_boto_download_patcher = mock.patch("redash_client.utils.s3.get_object")
    mock_download = mock_boto_download_patcher.start()

    # Make a temp file for returning
    temp_file = tempfile.mkstemp()
    file_handle = open(temp_file[1], "w+")
    file_handle.write(EXPECTED_JSON)
    file_handle.seek(0)

    mock_download.return_value = {"Body": file_handle}

    json_result = read_experiment_definition_s3("boop")

    self.assertEqual(json_result, json.loads(EXPECTED_JSON))

    mock_boto_download_patcher.stop()
    file_handle.close()

  def test_date_format(self):
    MS_DATE = 1493671545000.0
    EXPECTED_FORMAT = '05/01/17'
    formatted_date = format_date(MS_DATE)

    self.assertEqual(formatted_date, EXPECTED_FORMAT)

  def test_is_old_date(self):
    new_datetime = datetime.today() - timedelta(days=1)

    MS_DATE_OLD = 1493671545000.0
    MS_DATE_NEW = calendar.timegm(new_datetime.utctimetuple()) * 1000.0

    is_old = is_old_date(MS_DATE_OLD)
    self.assertEqual(is_old, True)

    is_old = is_old_date(MS_DATE_NEW)
    self.assertEqual(is_old, False)
