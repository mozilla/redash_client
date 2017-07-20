class RetentionType:
  DAILY = "day"
  WEEKLY = "week"


class TimeInterval:
  DAILY = "daily"
  WEEKLY = "weekly"
  MONTHLY = "monthly"
  allowed_time_intervals = [DAILY, WEEKLY, MONTHLY]


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
  allowed_chart_types = [BAR, PIE, LINE, SCATTER, AREA]


TTableSchema = [
    {
        "name": "Metric",
        "type": "string",
        "friendly_name": "Metric"
    }, {
        "name": "Alpha Error",
        "type": "float",
        "friendly_name": "Alpha Error"
    }, {
        "name": "Power",
        "type": "float",
        "friendly_name": "Power"
    }, {
        "name": "Two-Tailed P-value (ttest)",
        "type": "float",
        "friendly_name": "Two-Tailed P-value (ttest)"
    }, {
        "name": "Significance",
        "type": "string",
        "friendly_name": "Significance"
    }, {
        "name": "Experiment Mean - Control Mean",
        "type": "float",
        "friendly_name": "Experiment Mean - Control Mean"
    }]
