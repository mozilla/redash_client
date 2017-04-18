import json
import time
import requests
from slugify import slugify
from urlparse import urljoin
from urllib import urlencode

from constants import VizType, VizWidth, ChartType, TimeInterval


class RedashClient(object):
  BASE_URL = "https://sql.telemetry.mozilla.org/api/"
  MAX_RETRY_COUNT = 5

  class RedashClientException(Exception):
    pass

  def __init__(self, api_key):
    self._api_key = api_key
    self._url_params = urlencode({"api_key": self._api_key})

  def get_slug(self, name):
    return slugify(name)

  def make_visualization_options(self, chart_type=None, viz_type=None,
                                 column_mapping=None, series_options=None,
                                 time_interval=None, stacking=None):

    # See the API doc for more details about visualization options:
    # https://people-mozilla.org/~ashort/redash_docs/api.html
    if viz_type == VizType.COHORT:
      return {
          "timeInterval": time_interval,
      }

    # It's a chart viz type.
    options = {
        "bottomMargin": 50,
        "columnMapping": column_mapping,
        "globalSeriesType": chart_type,
        "legend": {"enabled": True},
        "series": {"stacking": "normal" if stacking else None},
        "seriesOptions": series_options if series_options else {},
        "sortX": True,
        "xAxis": {"type": "datetime", "labels": {"enabled": True}},
        "yAxis": [{"type": "linear"}, {"type": "linear", "opposite": True}],
    }

    return options

  def _make_request(self, request_function, url, args={}):
    if not request_function:
      request_function = requests.post

    try:
      response = request_function(url, args)
    except requests.RequestException as e:
      raise self.RedashClientException(
          ("Unable to communicate with redash: {error}").format(error=e), e)

    if response.status_code != 200:
      raise self.RedashClientException(
          ("Error status returned: {error_code} {error_message}").format(
              error_code=response.status_code,
              error_message=response.content,
          ), response.status_code)
    try:
      json_result = json.loads(response.content), response
    except ValueError as e:
      raise self.RedashClientException(
          ("Unable to parse JSON response: {error}").format(error=e))

    return json_result

  def _get_new_query_id(self, name, sql_query, data_source_id, description):
    url_path = "queries?{0}".format(self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    new_query_args = json.dumps({
        "name": name,
        "query": sql_query,
        "data_source_id": data_source_id,
        "description": description,
    })

    json_result, response = self._make_request(
        requests.post, query_url, new_query_args)

    query_id = json_result.get("id", None)
    return query_id

  def _get_table_id(self, query_id):
    url_path = "queries/{0}?{1}".format(str(query_id), self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    query_json_data, response = self._make_request(requests.get, query_url)
    query_visualizations = query_json_data.get("visualizations", [])
    table_id = None
    if len(query_visualizations) >= 1:
      table_id = query_visualizations[0].get("id", None)

    return table_id

  def create_new_query(self, name, sql_query,
                       data_source_id, description=None):
    query_id = self._get_new_query_id(
        name, sql_query, data_source_id, description)

    # If we can't get a query ID, the query has no table. Exit now.
    if not query_id:
      return None, None

    table_id = self._get_table_id(query_id)

    # Refresh our new query so it becomes available
    url_path = "queries/{0}/refresh?{1}".format(
        str(query_id), self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)
    self._make_request(requests.post, query_url)

    return query_id, table_id

  def get_query_results(self, sql_query, data_source_id):
    url_path = "query_results?{0}".format(self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    get_query_results_args = json.dumps({
        "query": sql_query,
        "data_source_id": data_source_id,
    })

    # If this query is still not uploaded, we'll get a job ID.
    # Let's retry in 1 second.
    for attempt in xrange(self.MAX_RETRY_COUNT):
      json_response, response = self._make_request(
          requests.post, query_url, get_query_results_args)
      if "job" not in json_response:
        break

      time.sleep(1)

    rows = json_response.get(
        "query_result", {}).get("data", {}).get("rows", [])
    return rows

  def create_new_visualization(self, query_id, viz_type=VizType.CHART,
                               title="", chart_type=None, column_mapping=None,
                               series_options=None, time_interval=None,
                               stacking=False):

    # Note: column_mapping is a dict of which field names to use for the x and
    # y axis. (e.g. {"event":"x","count":"y","type":"series"})
    if viz_type == VizType.CHART and (
       chart_type not in ChartType.allowed_chart_types or
       column_mapping is None):

      raise ValueError(("chart_type and column_mapping "
                        "values required for a Chart visualization"))

    # Note: time_interval is one of "daily", "weekly", "monthly"
    if (viz_type == VizType.COHORT and
       time_interval not in TimeInterval.allowed_time_intervals):

      raise ValueError(("time_interval value required for "
                        "a Cohort visualization"))

    if viz_type != VizType.CHART and viz_type != VizType.COHORT:
      raise ValueError("VizType must be one of: VizType.CHART, VizType.COHORT")

    url_path = "visualizations?{0}".format(self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    options = self.make_visualization_options(
        chart_type, viz_type, column_mapping,
        series_options, time_interval, stacking)

    new_visualization_args = json.dumps({
        "type": viz_type,
        "name": title,
        "options": options,
        "query_id": query_id,
    })

    json_result, response = self._make_request(
        requests.post, query_url, new_visualization_args)
    visualization_id = json_result.get("id", None)
    return visualization_id

  def create_new_dashboard(self, name):
    slug = self.get_slug(name)

    # Check if dashboard exists
    url_path = "dashboards/{0}?{1}".format(slug, self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    new_dashboard_args = json.dumps({"name": name})

    try:
      json_result, response = self._make_request(
          requests.get, query_url, new_dashboard_args)
    except self.RedashClientException as ex:
      server_error_code = ex.args[1]
      if server_error_code == 404:
        url_path = "dashboards?{0}".format(self._url_params)
        query_url = urljoin(self.BASE_URL, url_path)

        json_result, response = self._make_request(
            requests.post, query_url, new_dashboard_args)

    dashboard_id = json_result.get("id", None)
    return dashboard_id

  def publish_dashboard(self, dash_id):
    url_path = "dashboards/{0}?{1}".format(str(dash_id), self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    publish_dashboard_args = json.dumps({"is_draft": False})

    self._make_request(requests.post, query_url, publish_dashboard_args)

  def remove_visualization(self, viz_id):
    url_path = "widgets/{0}?{1}".format(str(viz_id), self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)
    self._make_request(requests.delete, query_url)

  def delete_query(self, query_id):
    url_path = "queries/{0}?{1}".format(str(query_id), self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)
    self._make_request(requests.delete, query_url)

  def add_visualization_to_dashboard(self, dash_id, viz_id, viz_width):
    if viz_width != VizWidth.REGULAR and viz_width != VizWidth.WIDE:
      raise ValueError(("viz_width should be one of "
                        "VizWidth.WIDE or VizWidth.REGULAR"))

    url_path = "widgets?{0}".format(self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    add_visualization_args = json.dumps({
        "dashboard_id": dash_id,
        "visualization_id": viz_id,
        "width": viz_width,
        "options": {},
        "text": "",
    })

    self._make_request(requests.post, query_url, add_visualization_args)

  def update_query_schedule(self, query_id, schedule):
    url_path = "queries/{0}?{1}".format(str(query_id), self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    update_query_args = json.dumps({"schedule": schedule, "id": query_id})

    self._make_request(requests.post, query_url, update_query_args)

  def get_widget_from_dash(self, name):
    slug = self.get_slug(name)
    url_path = "dashboards/{0}?{1}".format(slug, self._url_params)
    query_url = urljoin(self.BASE_URL, url_path)

    # Note: row_arr is in the form:
    # [[{}, {}], [{}], ...]
    #
    # Where each sub-array represents a row of widgets in a redash dashboard

    get_widget_args = json.dumps({"name": name})

    json_result, response = self._make_request(
        requests.get, query_url, get_widget_args)
    row_arr = json_result.get("widgets", [])

    # Return a flattened list of all widgets
    widgets = []
    for row in row_arr:
      if len(row) == 1:
        widgets.append(row[0])
      else:
        widgets += [row[0], row[1]]

    return widgets
