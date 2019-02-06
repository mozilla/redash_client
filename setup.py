import os
from setuptools import setup

_here = os.path.dirname(__file__)

setup(
  name = 'redash_client',
  packages = ['redash_client'],
  version = '0.2.5',
  description = 'A client for the Redash API for stmo (https://sql.telemetry.mozilla.org)',
  long_description= open(os.path.join(_here, 'README.rst')).read(),
  author = 'Marina Samuel',
  author_email = 'msamuel@mozilla.com',
  url = 'https://github.com/mozilla/redash_client',
  keywords = ['redash', 'experiments', 'a/b tests'],
  classifiers = [],
  install_requires=[
    "requests == 2.21.0",
    "python-slugify == 1.2.4",
    "urllib3 == 1.24.1"
  ]
)
