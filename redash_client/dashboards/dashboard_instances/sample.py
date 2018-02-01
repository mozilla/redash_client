import os

from redash_client.client import RedashClient
from redash_client.dashboards.StatisticalDashboard import StatisticalDashboard

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
'''
    "Activity Stream System Addon Experiment",
    "v1 Nightly Pocket User Personalization",
    "as-nightly-personalization-1400890",
    start_date="2017-09-27",
'''
'''
    "Activity Stream System Addon Experiment",
    "v2 Beta",
    "pref-flip-activity-stream-beta-1389722-v2",
    start_date="2017-08-30",
    end_date="2017-09-09"
'''
'''
    "Activity Stream System Addon Experiment",
    "v1 Release",
    "pref-flip-activity-stream-56-release-bug-1405332",
    start_date="2017-10-05",
    end_date="2017-10-20"
'''
'''
    "Activity Stream System Addon Experiment",
    "v1 About Home",
    "pref-flip-activity-stream-56-beta-about-home-bug-1405334",
    start_date="2017-10-05",
'''
'''
    "Activity Stream System Addon Experiment",
    "v2 Pocket Personalization",
    "pref-flip-activity-stream-58-nightly-pocket-personalization-bug-1400890",
    start_date="2017-10-06",
'''
'''
    "Activity Stream System Addon Experiment",
    "Beta Revisited",
    "pref-flip-activity-stream-beta-1389722-v2",
    start_date="2017-08-30",
    end_date="2017-09-08"
'''
'''
    "Activity Stream System Addon Experiment",
    "Release enUS",
    "pref-flip-activity-stream-56-release-bug-1405332",
    start_date="2017-10-05",
    end_date="2017-10-20"
'''
'''
    "Activity Stream System Addon Experiment",
    "Beta Post Bug Fix",
    "pref-flip-activity-stream-beta-1389722-v2-round2",
    start_date="2017-09-19",
    end_date="2017-09-24"
'''
'''
    "Activity Stream System Addon Experiment",
    "Beta All Pocket Geos Post Bug Fix",
    "pref-flip-activity-stream-beta-1389722-v2-2-round2",
    start_date="2017-09-20",
    end_date="2017-09-28"
'''
'''
    "Activity Stream System Addon Experiment",
    "Beta 57 Study",
    "pref-flip-activity-stream-57-beta-enabled-bug-1410535",
    start_date="2017-10-25",
'''
'''
    "Activity Stream System Addon Experiment",
    "Beta 57 Two Rows of Topsites",
    "pref-flip-activity-stream-57-beta-two-rows-bug-1411695",
    start_date="    ",
'''
'''
    "Activity Stream System Addon Experiment",
    "Beta 57 Two Rows v2",
    "pref-flip-activity-stream-57-beta-two-rows-user-pref-bug-1411695",
    start_date="2017-10-31",
'''
'''
    "Activity Stream System Addon Experiment",
    "v3 Pocket Personalization",
    "pref-flip-activity-stream-58-nightly-optimized-pocket-personalization-bug-1410483",
    start_date="2017-10-31",
'''
'''
    "Activity Stream System Addon Experiment",
    "57 Release",
    "pref-flip-activity-stream-57-release-enabled-existing-users-bug-1415966",
    start_date="2017-11-14"
'''
'''
    "Activity Stream System Addon Experiment",
    "57 Release New Users",
    "pref-flip-activity-stream-57-release-enabled-new-users-bug-1415967",
    start_date="2017-11-14"
'''

if __name__ == '__main__':
  api_key = os.environ["REDASH_API_KEY"]
  aws_access_key = os.environ['AWS_ACCESS_KEY']
  aws_secret_key = os.environ['AWS_SECRET_KEY']
  s3_bucket_id_stats = os.environ['S3_BUCKET_ID_STATS']

  redash_client = RedashClient(api_key)

  PING_CENTRE_TTABLE = "Statistical Analysis - Ping Centre"
  UT_TTABLE = "Statistical Analysis - UT"
  UT_HOURLY_TTABLE = "Statistical Analysis (Per Active Hour) - UT"

  dash = StatisticalDashboard(
    redash_client,
    "Activity Stream System Addon Experiment",
    "57 Release",
    "pref-flip-activity-stream-57-release-enabled-existing-users-bug-1415966",
    start_date="2017-11-14"
  )

  # Average Events per Day UT
  #dash.add_graph_templates("AS Template UT One:", dash.UT_EVENTS)
  #dash.add_graph_templates("AS Template UT Mapped Two:", dash.MAPPED_UT_EVENTS)

  # Average Events per Active Hour UT
  dash.add_graph_templates("AS Template UT Three:", dash.UT_HOURLY_EVENTS)
  dash.add_graph_templates("AS Template UT Mapped Four:", dash.MAPPED_UT_EVENTS)

  # Average Events per Day Ping Centre
  #dash.add_graph_templates("ASSA Template:", dash.DEFAULT_EVENTS)

  #dash.add_ttable_data("TTests Template UT Four:", UT_TTABLE, dash.UT_EVENTS)
  #dash.add_ttable_data("TTests Template Mapped UT Six:", UT_TTABLE, dash.MAPPED_UT_EVENTS)

  #dash.add_ttable(UT_TTABLE)

  # Events per Hour TTests
  #dash.add_ttable_data("TTests Template Per Hour UT Five:", UT_HOURLY_TTABLE, dash.UT_HOURLY_EVENTS)
  #dash.add_ttable_data("TTests Template Per Hour Mapped UT:", UT_HOURLY_TTABLE, dash.MAPPED_UT_EVENTS)

  #dash.add_ttable(UT_HOURLY_TTABLE)

  #dash.add_ttable_data("TTests Template:", PING_CENTRE_TTABLE, dash.DEFAULT_EVENTS)
  #dash.add_ttable(PING_CENTRE_TTABLE)

  #dash.update_refresh_schedule(86400)
  #dash.remove_all_graphs()
