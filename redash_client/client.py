import json
import time
import logging
import requests
from slugify import slugify

# Taking into account different versions of Python
try:  # pragma: no cover
  from urllib import urlencode
  from urlparse import urljoin
except ImportError:  # pragma: no cover
  from urllib.parse import urlencode
  from urllib.parse import urljoin

from redash_client.constants import VizType, VizWidth, ChartType, TimeInterval


class RedashClient(object):
  BASE_URL = "https://sql.telemetry.mozilla.org/"
  API_BASE_URL = BASE_URL + "api/"
  MAX_RETRY_COUNT = 5

  class RedashClientException(Exception):
    pass

  def __init__(self, api_key):
    self._api_key = api_key
    self._url_params = {"api_key": self._api_key}
    self._retry_delay = 1

    logging.basicConfig()
    self._logger = logging.getLogger()
    self._logger.setLevel(logging.INFO)

  def get_slug(self, name):
    return slugify(name)

  def make_visualization_options(
      self,
      chart_type=None,
      viz_type=None,
      column_mapping=None,
      series_options=None,
      time_interval=None,
      stacking=None,
      axis_info={}
  ):

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
        "sortX": axis_info.get("sortX", True),
        "xAxis": {
            "type": axis_info.get("x_axis_type", "datetime"),
            "labels": {"enabled": True}
        },
        "yAxis": [
            {"type": axis_info.get("y_axis_type", "linear")},
            {"type": "linear", "opposite": True}
        ],
    }

    return options

  def _make_request(self, request_function, url, req_args={}):
    if not request_function:
      request_function = requests.post

    try:
      if request_function != requests.post:
        response = request_function(url)
      else:
        response = request_function(url, req_args)
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
      json_result = response.json(), response
    except ValueError as e:
      raise self.RedashClientException(
          ("Unable to parse JSON response: {error}").format(error=e))

    return json_result

  def _make_api_request(self, request_function, url_path, req_args={}):
    req = requests.models.PreparedRequest()
    req_url = urljoin(self.API_BASE_URL, url_path)
    req.prepare_url(req_url, self._url_params)
    return self._make_request(request_function, req.url, req_args)

  def _get_new_query_id(self, name, sql_query, data_source_id, description):
    url_path = "queries"

    new_query_args = json.dumps({
        "name": name,
        "query": sql_query,
        "data_source_id": data_source_id,
        "description": description,
    })

    json_result, response = self._make_api_request(
        requests.post, url_path, new_query_args)

    query_id = json_result.get("id", None)
    return query_id

  def _get_visualization(self, query_id):
    url_path = "queries/{0}".format(str(query_id))

    query_json_data, response = self._make_api_request(requests.get, url_path)
    query_visualizations = query_json_data.get("visualizations", [])

    visualization_data = None
    if len(query_visualizations) >= 1:
      visualization_data = query_visualizations[0]

    return visualization_data

  def _refresh_graph(self, query_id):
    # Refresh our new query so it becomes available
    url_path = "queries/{0}/refresh".format(str(query_id))
    self._make_api_request(requests.post, url_path)

  def get_data_sources(self):
      url_path = "data_sources"
      json_response, response = self._make_api_request(requests.get, url_path)
      return json_response

  def create_new_query(self, name, sql_query,
                       data_source_id, description=None):
    query_id = self._get_new_query_id(
        name, sql_query, data_source_id, description)

    # If we can't get a query ID, the query has no table. Exit now.
    if not query_id:
      return None, None

    visualization = self._get_visualization(query_id)

    table_id = None
    if visualization:
      table_id = visualization.get("id", None)

    self._refresh_graph(query_id)

    return query_id, table_id

  def _poll_job(self, job):
    for attempt in range(self.MAX_RETRY_COUNT):
      url_path = "jobs/{}".format(job['id'])
      json_response, response = self._make_api_request(requests.get, url_path)
      job = json_response.get('job', None)

      # If the status shows the job is done processing
      # TODO: Add better comment for these statuses.
      if job.get('status', None) in (3, 4):
          break
      time.sleep(self._retry_delay)

    if job['status'] == 3:
      return job['query_result_id']
    return None

  def get_query_results(self, sql_query, data_source_id):
    url_path = "query_results"

    get_query_results_args = json.dumps({
        "query": sql_query,
        "data_source_id": data_source_id,
    })

    # If there aren't yet results, we'll get a job ID, so we poll for job
    # completion and then get the results when they're ready.
    json_response, response = self._make_api_request(
        requests.post, url_path, get_query_results_args)
    if "job" in json_response:
      result_id = self._poll_job(json_response['job'])
      if result_id:
        url_path = "{}/{}".format(url_path, result_id)
        json_response, response = self._make_api_request(
            requests.get, url_path)

    rows = json_response.get(
        "query_result", {}).get("data", {}).get("rows", [])
    return rows

  def make_new_visualization_request(self, query_id, viz_type, options, title):
    url_path = "visualizations"

    new_visualization_args = json.dumps({
        "type": viz_type,
        "name": title,
        "options": options,
        "query_id": query_id,
    })

    json_result, response = self._make_api_request(
        requests.post, url_path, new_visualization_args)
    visualization_id = json_result.get("id", None)
    return visualization_id

  def create_new_visualization(
      self,
      query_id, viz_type=VizType.CHART,
      title="Chart",
      chart_type=None,
      column_mapping=None,
      series_options=None,
      time_interval=None,
      stacking=False,
      axis_info={}
  ):

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

    options = self.make_visualization_options(
        chart_type, viz_type, column_mapping,
        series_options, time_interval, stacking, axis_info)

    visualization_id = self.make_new_visualization_request(
        query_id, viz_type, options, title)
    return visualization_id

  def create_new_dashboard(self, name):
    slug = self.get_slug(name)

    # Check if dashboard exists
    url_path = "dashboards/{0}".format(slug)

    new_dashboard_args = json.dumps({"name": name})

    try:
      json_result, response = self._make_api_request(
          requests.get, url_path)
      self._logger.info((
          "RedashClient: Dashboard {name} exists and has "
          "been fetched").format(name=name))
    except self.RedashClientException as ex:
      server_error_code = ex.args[1]
      if server_error_code == 404:
        self._logger.info((
            "RedashClient: Dashboard {name} does not exist. "
            "Creating a new one.").format(name=name))
        url_path = "dashboards"

        json_result, response = self._make_api_request(
            requests.post, url_path, new_dashboard_args)

    slug = json_result.get("slug", None)
    url_path = "dashboard/{slug}".format(slug=slug)
    dash_info = {
        "dashboard_id": json_result.get("id", None),
        "dashboard_slug": slug,
        "slug_url": None if slug is None else urljoin(self.BASE_URL, url_path)
    }
    return dash_info

  def get_public_url(self, dash_id):
    url_path = "dashboards/{}/share".format(str(dash_id))

    json_result, response = self._make_api_request(requests.post, url_path)
    public_url = json_result.get("public_url", None)
    return public_url

  def publish_dashboard(self, dash_id):
    url_path = "dashboards/{}".format(str(dash_id))

    publish_dashboard_args = json.dumps({"is_draft": False})

    self._make_api_request(requests.post, url_path, publish_dashboard_args)

  def remove_visualization(self, viz_id):
    url_path = "widgets/{}".format(str(viz_id))
    self._make_api_request(requests.delete, url_path)

  def delete_query(self, query_id):
    url_path = "queries/{}".format(str(query_id))
    self._make_api_request(requests.delete, url_path)

  def add_visualization_to_dashboard(self, dash_id, viz_id, viz_width):
    if viz_width != VizWidth.REGULAR and viz_width != VizWidth.WIDE:
      raise ValueError(("viz_width should be one of "
                        "VizWidth.WIDE or VizWidth.REGULAR"))

    url_path = "widgets"

    add_visualization_args = json.dumps({
        "dashboard_id": dash_id,
        "visualization_id": viz_id,
        "width": viz_width,
        "options": {},
        "text": "",
    })

    self._make_api_request(requests.post, url_path, add_visualization_args)

  def get_visualization_public_url(self, query_id, widget_id):
    url_params = urlencode(self._url_params)
    url_path = ("embed/query/{query_id}/visualization/{viz_id}"
                "?{url_param}").format(
        query_id=query_id, viz_id=widget_id, url_param=url_params)
    query_url = urljoin(self.BASE_URL, url_path)
    return query_url

  def update_query_schedule(self, query_id, schedule):
    url_path = "queries/{}".format(str(query_id))

    update_query_args = json.dumps({"schedule": schedule, "id": query_id})

    self._make_api_request(requests.post, url_path, update_query_args)

  def update_query(self, query_id, name, sql_query,
                   data_source_id, description, options=None):
    url_path = "queries/{0}".format(str(query_id))

    update_query_args = {
        "data_source_id": data_source_id,
        "query": sql_query,
        "name": name,
        "description": description,
        "id": query_id,
    }

    if options:
      update_query_args["options"] = options

    self._make_api_request(requests.post, url_path,
                           json.dumps(update_query_args))
    self._refresh_graph(query_id)

  def fork_query(self, query_id):
    url_path = "queries/{0}/fork".format(query_id)

    json_result, response = self._make_api_request(
        requests.post, url_path)
    fork = {
        "id": json_result.get("id", None),
        "query": json_result.get("query", None),
        "data_source_id": json_result.get("data_source_id", None)
    }

    return fork

  def search_queries(self, keyword):
    url_path = "queries?q={0}".format(keyword)

    json_result, response = self._make_api_request(
        requests.get, url_path)

    templated_queries = []
    for query in json_result["results"]:
      query_id = query.get("id", None)
      visualization = self._get_visualization(query_id)
      options = visualization.get("options", None)
      viz_type = visualization.get("type", None)

      templated_queries.append({
          "id": query_id,
          "description": query.get("description", None),
          "name": query.get("name", None),
          "data_source_id": query.get("data_source_id", None),
          "options": options,
          "type": viz_type,
          "query": query.get("query", None)
      })

    return templated_queries

  def get_widget_from_dash(self, name):
    slug = self.get_slug(name)
    url_path = "dashboards/{0}".format(slug)

    # Note: row_arr is in the form:
    # [{}, {}, {} ...]
    #
    # Where each object represents a widget in a redash dashboard

    json_result, response = self._make_api_request(requests.get, url_path)
    widgets = json_result.get("widgets", [])
    return widgets
