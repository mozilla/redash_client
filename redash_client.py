import json
import string
import requests
import itertools
import sched, time
from constants import VizType, ChartType

class RedashClient(object):
  BASE_URL = "https://sql.telemetry.mozilla.org/api"

  def __init__(self, api_key):
    self.api_key = api_key
    self.s = sched.scheduler(time.time, time.sleep)

  def new_query(self, name, query_string, data_source_id, description=""):
    query_id = requests.post(
      self.BASE_URL + "/queries?api_key=" + self.api_key,
      data = json.dumps({"name": name,
                         "query": query_string,
                         "data_source_id": data_source_id,
                         "description": description})
    ).json()["id"]

    table_id = requests.get(
      self.BASE_URL + "/queries/" + str(query_id) + "?api_key=" + self.api_key
    ).json()["visualizations"][0]["id"]

    requests.post(
      self.BASE_URL + "/queries/" + str(query_id) + "/refresh?api_key=" + self.api_key
    ).json()

    return query_id, table_id

  def get_query_results(self, query_string, data_source_id):
    response = requests.post(
      self.BASE_URL + "/query_results?api_key=" + self.api_key,
      data = json.dumps({"query": query_string, "data_source_id": data_source_id}),
    ).json()

    # If this query is still not uplodaded, we'll get a job ID. Let's retry in 1 second.
    if ("job" in response.keys()):
      self.s.enter(1, 1, self.get_query_results, (query_string, data_source_id,))
      self.s.run()
    else:
      return response["query_result"]["data"]["rows"]

  def new_visualization(self, query_id, viz_type=VizType.CHART, title="",
    chart_type=None, column_mapping=None, series_options=None, time_interval=None, stacking=False):
    """ Create a new Redash Visualization.

    Keyword arguments:

    query_id -- the id returned when calling new_query()
    viz_type (optional) -- one of the VizType constants (CHART|COHORT)
    title (optional) -- title of your visualization
    chart_type (optional) -- one of the ChartType constants (BAR|PIE|LINE|SCATTER|AREA)
      - applies only to VizType.CHART
    column_mapping (optional) -- a dict of which field names to use for the x and
      y axis. (e.g. {"event":"x","count":"y","type":"series"})
      - applies only to VizType.CHART
    time_interval (optional) -- one of "daily", "weekly", "monthly"
      - applies only to VizType.COHORT
    """

    options = {}

    if viz_type == VizType.CHART:
      if chart_type == None or column_mapping == None:
        raise ValueError("chart_type and column_mapping values required for a Chart visualization")

      options = {
        "globalSeriesType": chart_type,
        "sortX":True,
        "legend": {"enabled":True},
        "yAxis": [{"type": "linear"}, {"type": "linear", "opposite":True}],
        "xAxis": {"type": "datetime","labels": {"enabled":True}},
        "seriesOptions": series_options,
        "columnMapping": column_mapping,
        "bottomMargin":50
      }

    options["series"] = { "stacking": "normal" if stacking else None }
    options["seriesOptions"] = series_options if series_options else {}

    if viz_type == VizType.COHORT:
      if time_interval == None:
        raise ValueError("time_interval value required for a Cohort visualization")

      options = {
        "timeInterval": time_interval
      }

    return requests.post(
      self.BASE_URL + "/visualizations?api_key=" + self.api_key, 
      data = json.dumps({
        "type": viz_type,
        "name": title,
        "options": options,
        "query_id": query_id}),
    ).json()["id"]

  def new_dashboard(self, name):
    """
    Create a new Redash dashboard. If a dashboard with the given name
    already exists, don't create a new one

    Keyword arguments:

    name -- a title for the dashboard
    """

    slug = name \
      .lower() \
      .replace("/", " ") \
      .translate(None, string.punctuation) \
      .replace(" ", "-")

    # Check if dashboard exists
    dash = requests.get(
      self.BASE_URL + "/dashboards/" + slug + "?api_key=" + self.api_key,
      data = json.dumps({"name": name}), 
    )

    # If dashboard doesn't exist, create a new one.
    if dash.status_code != 200:
      return requests.post(
        self.BASE_URL + "/dashboards?api_key=" + self.api_key,
        data = json.dumps({"name": name}),
      ).json()["id"]

    return dash.json()["id"]

  def publish_dashboard(self, dash_id):
    requests.post(
      self.BASE_URL + "/dashboards/" + str(dash_id) + "?api_key=" + self.api_key,
      data = json.dumps({"is_draft": False})
    )

  def remove_visualization(self, dash_name, viz_id):
    requests.delete(
      self.BASE_URL + "/widgets/" + str(viz_id) + "?api_key=" + self.api_key
    )

  def append_viz_to_dash(self, dash_id, viz_id, viz_width):
    requests.post(
      self.BASE_URL + "/widgets?api_key=" + self.api_key,
      data = json.dumps({
        "dashboard_id": dash_id,
        "visualization_id": viz_id,
        "width": viz_width,
        "options":{},
        "text":""
      }), 
    )

  def update_query_schedule(self, query_id, schedule):
    requests.post(
      self.BASE_URL + "/queries/" + str(query_id) + "?api_key=" + self.api_key,
      data = json.dumps({"schedule": schedule, "id": query_id})
    )

  def get_widget_from_dash(self, name):
    slug = name \
      .lower() \
      .replace("/", " ") \
      .translate(None, string.punctuation) \
      .replace(" ", "-")

    row_arr = requests.get(
      self.BASE_URL + "/dashboards/" + slug + "?api_key=" + self.api_key,
      data = json.dumps({"name": name}),
    ).json()["widgets"]

    return list(itertools.chain.from_iterable(map(lambda row: [row[0]] if len(row) == 1 else [row[0], row[1]], row_arr)))
