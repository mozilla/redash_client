class RetentionType:
  DAILY = 'day'
  WEEKLY = 'week'

class VizType:
  CHART = "CHART"
  COHORT = "COHORT"

class VizWidth:
  REGULAR = 1
  WIDE = 2

class ChartType:
  BAR = "column"
  PIE = "pie"
  LINE = "line"
  SCATTER = "scatter"
  AREA = "area"

TtableSchema = [
  {
    "name": "Metric",
    "type": "string",
    "friendly_name": "Metric"
  },
  {
    "name": "Alpha Error",
    "type": "float",
    "friendly_name": "Alpha Error"
  },
  {
    "name": "Power",
    "type": "float",
    "friendly_name": "Power"
  },
  {
    "name": "Two-Tailed P-value (ttest)",
    "type": "float",
    "friendly_name": "Two-Tailed P-value (ttest)"
  },
  {
    "name": "Experiment Mean - Control Mean",
    "type": "float",
    "friendly_name": "Experiment Mean - Control Mean"
  }
]