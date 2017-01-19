import os
from redash_client import RedashClient
from constants import VizType, ChartType, VizWidth

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  redash = RedashClient(api_key)
  
  '''
  query_id = redash.new_query(
    "TEST",
    "SELECT event, COUNT(*) FROM activity_stream_masga GROUP BY event",
    5)
  '''

  # viz_id = redash.new_visualization(2293, "hello", VizType.CHART, ChartType.BAR)
  # print "viz id " + str(viz_id)

  # dashboard_id = redash.new_dashboard("TestDash")

  redash.append_viz_to_dash(157, 4206, VizWidth.REGULAR)
