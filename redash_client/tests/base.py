import mock
import json
import unittest


class AppTest(unittest.TestCase):

  def get_mock_response(self, status=200, content='{}'):
    mock_response = mock.Mock()
    mock_response.status_code = status
    mock_response.content = content

    return mock_response
