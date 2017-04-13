lint:
	flake8 src/redash_client.py

test: lint
	nosetests --with-coverage --cover-package=src
