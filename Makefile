lint:
	flake8 src/constants.py
	flake8 src/redash_client.py
	flake8 src/samples/SummaryDashboard.py
	flake8 src/samples/ActivityStreamExperimentDashboard.py
	flake8 src/tests/test_redash.py
	flake8 src/tests/test_summary_dashboard.py

test: lint
	nosetests --with-coverage --cover-package=src
