import json
import string
import requests
import itertools
from constants import VizType, ChartType

class RedashClient(object):
  BASE_URL = "https://sql.telemetry.mozilla.org/api"

  def __init__(self, api_key):
    self.api_key = api_key

  def new_query(self, name, query_string, data_source_id):
    query_id = requests.post(
      self.BASE_URL + "/queries?api_key=" + self.api_key,
      data = json.dumps({"name": name, "query": query_string, "data_source_id": data_source_id}), 
    ).json()["id"]

    requests.post(
      self.BASE_URL + "/queries/" + str(query_id) + "/refresh?api_key=" + self.api_key
    ).json()

    return query_id

  def new_visualization(self, query_id, chart_type, column_mapping, title="", viz_type=VizType.CHART):
    """ Create a new Redash Visualization.

    Keyword arguments:

    query_id -- the id returned when calling new_query()
    chart_type -- one of the ChartType constants (BAR|PIE|LINE|SCATTER)
    column_mapping -- a dict of which field names to use for the x and y axis. (e.g. {"event":"x","count":"y","type":"series"})
    title (optional) -- title of your visualization
    viz_type (optional) -- one of the VizType constants (CHART|<TBD>)
    """

    options = {
      "globalSeriesType": chart_type,
      "sortX":True,
      "legend": {"enabled":True},
      "yAxis": [{"type": "linear"}, {"type": "linear", "opposite":True}],
      "xAxis": {"type": "datetime","labels": {"enabled":True}},
      "seriesOptions":{"count": {
        "type": chart_type,
        "yAxis": 0,
        "zIndex":0,
        "index":0
      }},
      "columnMapping": column_mapping,
      "bottomMargin":50
    }

    return requests.post(
      self.BASE_URL + "/visualizations?api_key=" + self.api_key, 
      data = json.dumps({
        "type": viz_type,
        "name": title,
        "options": options,
        "query_id":query_id}),
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
