import os
from samples.ActivityStreamExperimentDashboard import ActivityStreamExperimentDashboard

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
    ['1.8.0'],
    "04/06/17"
'''
'''
    "Screenshots Long Cache",
    "exp-014-screenshotsasync",
    ['1.8.0'],
    "04/06/17"
'''

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  dash = ActivityStreamExperimentDashboard(
    api_key,
    "Deduped Combined Frecency",
    "exp-006-deduped-combined-frecency",
    ['1.2.0', '1.3.0'],
    "01/18/17"
  )

  dash.add_retention_diff()
  dash.add_event_graphs(dash.DEFAULT_EVENTS)
  dash.add_event_graphs(dash.MASGA_EVENTS, events_table="activity_stream_masga")
  dash.add_events_per_user(dash.DEFAULT_EVENTS)
  dash.add_disable_graph()
  dash.add_ttable()
  dash.update_refresh_schedule(86400)
