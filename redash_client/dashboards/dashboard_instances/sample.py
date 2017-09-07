import os

from redash_client.client import RedashClient
from redash_client.dashboards.ActivityStreamExperimentDashboard import ActivityStreamExperimentDashboard

'''
    "Deduped Combined Frecency",
    "exp-006-deduped-combined-frecency",
    ['1.2.0', '1.3.0', '1.4.0', '1.4.1'],
    "01/18/17"
'''
'''
    "Original Newtab Sites",
    "exp-008-original-newtab-sites",
    ['1.3.0', '1.4.0', '1.4.1'],
    "02/02/17"
'''
'''
    "Locally Fetch Metadata",
    "exp-007-locally-fetch-metadata",
    ['1.3.0', '1.4.0'],
    "02/02/17"
'''
'''
    "Locally Fetch Metadata",
    "exp-010-locally-fetch-metadata",
    ['1.4.1', '1.5.0', '1.6.0'],
    "02/15/17"
'''
'''
    "Screenshots",
    "exp-009-screenshots",
    ['1.5.0', '1.6.0'],
    "02/23/17"
'''
'''
    "Async Screenshots",
    "exp-012-screenshotsasync",
    ['1.7.0'],
    "03/20/17"
'''
'''
    "Bookmark Screenshots",
    "exp-013-bookmark-screenshots",
    ['1.8.0'],
    "04/06/17"
'''
'''
    "Metadata Long Cache",
    "exp-015-metadatalongcache",
    ['1.8.0', '1.9.0'],
    "04/06/17"
'''
'''
    "Screenshots Long Cache",
    "exp-014-screenshotsasync",
    ['1.8.0'],
    "04/06/17"
'''
'''
    "Pocket",
    "exp-021-pocketstories",
    ['1.10.1'],
    "05/02/17"
'''
'''
    "Metadata No Service",
    "exp-018-metadata-no-service",
    ['1.10.0', '1.10.1'],
    "05/01/17"
'''
'''
    "Metadata Local Refresh",
    "exp-019-metadata-local-refresh",
    ['1.10.0', '1.10.1'],
    "05/01/17"
'''

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  redash_client = RedashClient(api_key)

  dash = ActivityStreamExperimentDashboard(
    redash_client,
    "Activity Stream Beta V2",
    "pref-flip-activity-stream-beta-1389722-v2",
    start_date="2017-08-29",
    end_date="2017-09-06"
  )

  dash.add_graph_templates("AS Template UT Mapped:", dash.MAPPED_UT_EVENTS)
  dash.add_graph_templates("AS Template UT:", dash.UT_EVENTS)
  dash.add_ttable("TTests Template UT:", dash.UT_EVENTS)
  #dash.update_refresh_schedule(86400)
  #dash.remove_all_graphs()
