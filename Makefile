build:
	./lambda.sh

lint:
	flake8 redash_client/constants.py
	flake8 redash_client/client.py
	flake8 redash_client/tests/test_redash.py

test: lint
	nosetests --with-coverage --cover-package=redash_client
