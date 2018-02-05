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
