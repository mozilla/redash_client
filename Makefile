build:
	./lambda.sh

lint:
	flake8 redash_client/constants.py
	flake8 redash_client/client.py
	flake8 redash_client/utils.py
	flake8 redash_client/dashboards/SummaryDashboard.py
	flake8 redash_client/dashboards/ActivityStreamExperimentDashboard.py
	flake8 redash_client/tests/base.py
	flake8 redash_client/tests/test_redash.py
	flake8 redash_client/tests/test_summary_dashboard.py
	flake8 redash_client/test/test_utils.py
	flake8 redash_client/tests/test_activity_stream_experiment_dashboard.py
	flake8 redash_client/tests/test_statistical_dashboard.py

test: lint
	nosetests --with-coverage --cover-package=redash_client
