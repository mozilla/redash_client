from distutils.core import setup
setup(
  name = 'redash_client',
  packages = ['redash_client', 'redash_client.dashboards'],
  version = '0.1.5',
  description = 'A client for the re:dash API for stmo (https://sql.telemetry.mozilla.org)',
  author = 'Marina Samuel',
  author_email = 'msamuel@mozilla.com',
  url = 'https://github.com/mozilla/redash_client',
  keywords = ['redash', 'experiments', 'a/b tests'],
  classifiers = [],
)