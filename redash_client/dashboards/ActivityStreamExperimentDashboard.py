import logging

from redash_client.constants import VizWidth
from redash_client.dashboards.SummaryDashboard import SummaryDashboard


class ActivityStreamExperimentDashboard(SummaryDashboard):
  # These are either strings representing both the measurement name
  # event being measured or a key value pair: {<measurement_name>: <events>}
  DEFAULT_EVENTS = ["CLICK", "SEARCH", "BLOCK", "DELETE",
                    {
                      "event_name": "Positive Interactions",
                      "event_list": ["CLICK", "BOOKMARK_ADD", "SEARCH"]}]
  UT_EVENTS = [
      "scalar_parent_browser_engagement_unique_domains_count",
      "scalar_parent_browser_engagement_active_ticks",
      "scalar_parent_browser_engagement_tab_open_event_count",
      "scalar_parent_browser_engagement_max_concurrent_tab_count",
      "scalar_parent_browser_engagement_unfiltered_uri_count"]
  UT_HOURLY_EVENTS = [
      "scalar_parent_browser_engagement_unique_domains_count",
      "scalar_parent_browser_engagement_tab_open_event_count",
      "scalar_parent_browser_engagement_max_concurrent_tab_count",
      "scalar_parent_browser_engagement_unfiltered_uri_count",]
  UT_MAPPED_HOURLY_EVENTS = [
      "scalar_parent_browser_engagement_navigation_searchbar",
      "scalar_parent_browser_engagement_navigation_about_newtab"
      "scalar_parent_browser_engagement_navigation_about_home"]
  MAPPED_UT_EVENTS = [
      "scalar_parent_browser_engagement_navigation_searchbar",
      "scalar_parent_browser_engagement_navigation_about_newtab",
      "scalar_parent_browser_engagement_navigation_about_home"]

  DEFAULT_EVENTS_TABLE = "assa_events_daily"
  URL_FETCHER_DATA_SOURCE_ID = 28
  DISABLE_TITLE = "Disable Rate"
  RETENTION_DIFF_TITLE = "Daily Retention Difference (Experiment - Control)"

  def __init__(self, redash_client, project_name, dash_name, exp_id,
               start_date=None, end_date=None):
    DASH_TITLE = "{project}: {dash}".format(
        project=project_name, dash=dash_name)
    super(ActivityStreamExperimentDashboard, self).__init__(
        redash_client,
        DASH_TITLE,
        self.DEFAULT_EVENTS_TABLE,
        start_date, end_date)

    logging.basicConfig()
    self._logger = logging.getLogger()
    self._logger.setLevel(logging.INFO)
    self._experiment_id = exp_id

    self._params["experiment_id"] = self._experiment_id
    self._logger.info((
        "ActivityStreamExperimentDashboard: {name} "
        "Initialization Complete".format(name=dash_name)))

  def _get_title(self, template_name):
    title = template_name.title().split(": ")
    if len(title) > 1:
      title = title[1]
    return title

  def _get_event_title_description(self, template, event):
    if type(event) == str:
      event_name = event.capitalize()
      event_string = "('{}')".format(event)
    else:
      event_name = event["event_name"]
      events = []
      for event in event["event_list"]:
        events.append("'{}'".format(event))
      event_string = "(" + ", ".join(events) + ")"

    self._params["event"] = event
    self._params["event_string"] = event_string
    title = description = self._get_title(template["name"]).replace(
        "Event", event_name)

    if template["description"]:
      description = template["description"].lower().replace(
          "event", event_name).capitalize()

    event_data = {
        "title": title,
        "description": description
    }
    return event_data

  def _create_options(self):
    options = {
        "parameters": []
    }

    for param in self._params:
      param_obj = {
          "title": param,
          "name": param,
          "type": "text",
          "value": self._params[param],
          "global": False
      }
      options["parameters"].append(param_obj)

    return options

  def _apply_non_event_template(self, template, chart_data, values=None):
    title = description = self._get_title(template["name"])

    if template["description"]:
      description = template["description"]

    self._add_template_to_dashboard(
        template,
        chart_data,
        title,
        VizWidth.WIDE,
        description
    )

  def _apply_event_template(self, template, chart_data,
                            events_list, events_table, title=None):
    for event in events_list:
      event_data = self._get_event_title_description(template, event)

      self._add_template_to_dashboard(
          template,
          chart_data,
          event_data["title"],
          VizWidth.REGULAR,
          event_data["description"],
      )

  def _add_template_to_dashboard(self, template, chart_data, title,
                                 viz_width, description):
    # Remove graphs if they already exist.
    if title in chart_data:
      self._logger.info(("ActivityStreamExperimentDashboard: "
                         "{template} graph exists and is being removed"
                         .format(template=title)))

      query_id = chart_data[title]["query_id"]
      widget_id = chart_data[title]["widget_id"]
      self.remove_graph_from_dashboard(widget_id, query_id)

    self._logger.info(("ActivityStreamExperimentDashboard: "
                       "New {title} graph is being added"
                       .format(title=title)))
    self._add_forked_query_to_dashboard(
        title,
        template["id"],
        self._params,
        viz_width,
        template["options"],
        template["type"],
        description
    )

  def _apply_functions_to_templates(
      self, template_keyword, events_list, events_table,
      events_function, general_function=None, title=None
  ):
    if events_table is None:
      events_table = self._events_table
    self._params["events_table"] = events_table

    templates = self.redash.search_queries(template_keyword)
    chart_data = self.get_query_ids_and_names()

    for template in templates:
      if "event" in template["name"].lower():
        self._logger.info((
            "ActivityStreamExperimentDashboard: "
            "Processing template '{template_name}'"
            .format(template_name=template["name"])))
        events_function(
            template,
            chart_data,
            events_list,
            events_table,
            title)
      else:
        self._logger.info((
            "ActivityStreamExperimentDashboard: "
            "Processing template '{template_name}'"
            .format(template_name=template["name"])))
        general_function(template, chart_data)

  def add_graph_templates(self, template_keyword,
                          events_list=None, events_table=None):
    self._logger.info(
        "ActivityStreamExperimentDashboard: Adding templates.")

    if events_list is None:
      events_list = self.DEFAULT_EVENTS

    self._apply_functions_to_templates(
        template_keyword,
        events_list,
        events_table,
        self._apply_event_template,
        self._apply_non_event_template
    )
