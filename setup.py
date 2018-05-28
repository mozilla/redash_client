from setuptools import setup
setup(
  name = 'redash_client',
  packages = ['redash_client'],
  description = 'A client for the Redash API for stmo (https://sql.telemetry.mozilla.org)',
  version = '0.2.3',
  author = 'Marina Samuel',
  author_email = 'msamuel@mozilla.com',
  url = 'https://github.com/mozilla/redash_client',
  keywords = ['redash', 'experiments', 'a/b tests'],
  classifiers = [],
  install_requires=[
    "requests == 2.12.1",
    "python-slugify == 1.2.4",
    "urllib3 == 1.21.1"
  ]
)