from redash_client import RedashClient

class SummaryDashboard(object):
  def __init__(self, api_key, dash_name):
    self._api_key = api_key
    self._dash_name = dash_name

    self.redash = RedashClient(api_key)
    self._dash_id = self.redash.new_dashboard(self._dash_name)
    self.redash.publish_dashboard(self._dash_id)
