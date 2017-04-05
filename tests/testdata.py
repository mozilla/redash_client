from constants import ChartType

EXPECTED_QUERY_ID = "query_id123"
EXPECTED_QUERY_ID2 = "query_id456"
EXPECTED_QUERY_ID3 = "query_id789"
EXPECTED_VIZ_ID = "viz_id123"
EXPECTED_ROWS = [{
  "col1": 123,
  "col2": 456,
},
{
  "col1": 789,
  "col2": 123,
}]

QUERY_ID_RESPONSE = {
  "id": EXPECTED_QUERY_ID
}

VISUALIZATION_LIST_RESPONSE = {
  "visualizations": [{
    "id": EXPECTED_VIZ_ID
  }]
}

FLAT_WIDGETS = [
{
  "visualization": {
    "query": {
      "id": EXPECTED_QUERY_ID
    }
  }
},
{
  "visualization": {
    "query": {
      "id": EXPECTED_QUERY_ID2
    }
  }
},
{
  "visualization": {
    "query": {
      "id": EXPECTED_QUERY_ID3
    }
  }
}]

WIDGETS_RESPONSE = {
  "widgets":[[{
    "visualization": {
      "query": {
          "id": EXPECTED_QUERY_ID
      }
    }}],
    [{"visualization": {
      "query": {
        "id": EXPECTED_QUERY_ID2
      }
    }},
    {"visualization": {
      "query": {
        "id": EXPECTED_QUERY_ID3
      }
    }}
  ]]
}

QUERY_RESULTS_RESPONSE = {
  "query_result": {
    "data": {
      "rows": EXPECTED_ROWS
    }
  }
}

QUERY_RESULTS_NOT_READY_RESPONSE = {
  "job": {}
}

TIME_INTERVAL = "weekly"
COHORT_OPTIONS = {
  "timeInterval": TIME_INTERVAL
}

COLUMN_MAPPING = {"date": "x", "event_rate": "y", "type": "series"}
CHART_OPTIONS = {
  "globalSeriesType": ChartType.LINE,
  "sortX":True,
  "legend": {"enabled":True},
  "yAxis": [{"type": "linear"}, {"type": "linear", "opposite":True}],
  "series": { "stacking":  None },
  "xAxis": {"type": "datetime","labels": {"enabled":True}},
  "seriesOptions": {},
  "columnMapping": COLUMN_MAPPING,
  "bottomMargin":50
}

DASH_NAME = "Activity Stream A/B Testing: Beep Meep"
EXPECTED_SLUG = "activity-stream-a-b-testing-beep-meep"
