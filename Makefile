lint:
	flake8 src/constants.py
	flake8 src/redash_client.py
	flake8 src/samples/SummaryDashboard.py
	flake8 src/samples/ActivityStreamExperimentDashboard.py
	flake8 src/tests/base.py
	flake8 src/tests/test_redash.py
	flake8 src/tests/test_summary_dashboard.py
	flake8 src/test/test_utils.py
	flake8 src/tests/test_activity_stream_experiment_dashboard.py

test: lint
	nosetests --with-coverage --cover-package=src
