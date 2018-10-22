.. image:: https://travis-ci.org/mozilla/redash_client.svg?branch=master
  :target: https://travis-ci.org/mozilla/redash_client

.. image:: https://coveralls.io/repos/github/mozilla/redash_client/badge.svg?branch=master
  :target: https://coveralls.io/github/mozilla/redash_client?branch=master

====================
Redash Python Client
====================

A client for the Redash API for `stmo <https://sql.telemetry.mozilla.org>`_.

Redash API
----------

Redash doesn't have a formal API but what it exposes internally for its
client-side app is the API this project attempts to track.
To get a list of all endpoints available, see
https://github.com/getredash/redash/blob/master/redash/handlers/api.py

=======
Install
=======

.. code-block:: bash

  pip install redash_client

=====
Usage
=====

Before using :code:`RedashClient`, set the :code:`REDASH_API_KEY` environment variable to your Redash API key:

:code:`export REDASH_API_KEY=<your_api_key>`

To import and use :code:`RedashClient`:

.. code:: python

  import os
  from redash_client.client import RedashClient

  api_key = os.environ["REDASH_API_KEY"]
  redash_client = RedashClient(api_key)

  # Make a Redash API call:
  redash_client.search_queries("AS Template:")


===============
Package for Pip
===============

First, you must update the :code:`version` field in :code:`setup.py`. Then run these commands:

.. code-block:: bash

  python setup.py sdist

  twine upload dist/redash_client-<version>.tar.gz
