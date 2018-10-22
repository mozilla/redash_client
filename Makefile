build:
	./lambda.sh

lint:
	flake8 redash_client

test:
	coverage run --source redash_client -m pytest
