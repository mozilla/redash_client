lint:
	flake8 src/constants.py
	flake8 src/redash_client.py
	flake8 src/tests/test_redash.py

test: lint
	nosetests --with-coverage --cover-package=src
