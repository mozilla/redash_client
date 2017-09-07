def retention(events_table, retention_type, start_date, where_clause):
  return """
  WITH population AS
      (SELECT client_id AS unique_id, DATE_TRUNC('{1}', date) AS cohort_date, COUNT(*)
       FROM {0}
       WHERE 1 = 1
       {3}
       GROUP BY 1, 2),

  activity AS
      (SELECT DATE_TRUNC('{1}', date) AS activity_date, client_id AS unique_id, cohort_date
       FROM {0}
       JOIN population
       ON population.unique_id = client_id
       WHERE DATE_TRUNC('{1}', date) >= (CURRENT_DATE - INTERVAL '91 days')
       AND DATE_TRUNC('{1}', cohort_date) >= (CURRENT_DATE - INTERVAL '91 days')
       {3}),

  population_agg AS
      (SELECT DATE_TRUNC('{1}', date) AS cohort_date, COUNT(DISTINCT client_id) AS total
       FROM {0}
       WHERE 1 = 1
       {3}
       GROUP BY 1)

  SELECT * FROM
      (SELECT date, day as week_number, value, total, MAX(day) over (PARTITION BY date) AS max_week_num
      FROM
          (SELECT activity.cohort_date AS date,
             DATE_DIFF('{1}', activity.cohort_date, activity_date) AS day,
             total,
             COUNT(DISTINCT unique_id) AS value
          FROM activity
          JOIN population_agg
          ON activity.cohort_date = population_agg.cohort_date
          WHERE activity_date >= activity.cohort_date
          AND activity.cohort_date > '{2}'
          GROUP BY 1, 2, 3))
  WHERE week_number < max_week_num
  ORDER BY date, week_number""".format(events_table, retention_type, start_date, where_clause), []

def all_events_weekly(events_table, start_date, where_clause, event_column):
  return """
    WITH weekly_events AS
      (SELECT DATE_TRUNC('week', date) AS week, COUNT(*)
      FROM {0}
      WHERE DATE_TRUNC('week', date) >= '{1}'
      {2}
      GROUP BY 1),

  event_counts AS
      (SELECT week, {3}, count FROM
          (SELECT *, RANK() over (PARTITION BY week ORDER BY count) AS rank FROM
              (SELECT DATE_TRUNC('week', date) AS week, {3}, COUNT(*)
              FROM {0}
              WHERE DATE_TRUNC('week', date) >= '{1}'
              {2}
              GROUP BY 1, 2
              ORDER BY 1, 2))
      WHERE rank <= 20)

  SELECT weekly_events.week, event_counts.{3}, event_counts.count / weekly_events.count::FLOAT * 100 AS rate
  FROM weekly_events
  LEFT JOIN event_counts
  ON weekly_events.week = event_counts.week""".format(events_table, start_date, where_clause, event_column), ["week", "rate", event_column]

def active_users(events_table, start_date, where_clause=""):
  return """
    WITH weekly AS
    (SELECT day, COUNT(DISTINCT client_id) AS dist_clients
    FROM
      (SELECT DISTINCT date
       FROM {0}
       WHERE date >= '{1}'
       {2}
       ORDER BY date) AS g(day)
    LEFT JOIN {0}
    ON {0}.date BETWEEN g.day - 7 AND g.day
    AND {0}.date >= '{1}'
    {2}
    GROUP BY day
    ORDER BY day),

    monthly AS
    (SELECT day, count(DISTINCT client_id) AS dist_clients
    FROM
      (SELECT DISTINCT date
       FROM {0}
       WHERE date >= '{1}'
       {2}
       ORDER BY date) AS g(day)
    LEFT JOIN {0}
    ON {0}.date BETWEEN g.day - 28 AND g.day
    AND {0}.date >= '{1}'
    {2}
    GROUP BY day
    ORDER BY day),

    daily AS
    (SELECT date, COUNT(DISTINCT a.client_id) AS dau
     FROM {0} AS a WHERE date >= '{1}' {2} GROUP BY date),

    smoothed_daily AS
    (SELECT date as day,
           dau,
           AVG(dau) OVER(order by date ROWS BETWEEN 7 PRECEDING AND 0 FOLLOWING) as dist_clients
    FROM daily
    ORDER BY day desc)

    SELECT
      date,
      d.dist_clients as dau,
      w.dist_clients as wau,
      m.dist_clients as mau,
      (d.dist_clients::FLOAT / w.dist_clients) * 100.0 as weekly_engagement,
      (d.dist_clients::FLOAT / m.dist_clients) * 100.0 as monthly_engagement
    FROM {0} a
    JOIN smoothed_daily d on d.day = date
    JOIN weekly w on w.day = date
    JOIN monthly m on m.day = date
    WHERE date < current_date and date >= '2016-05-10'
    GROUP BY date, d.dist_clients, wau, mau
    ORDER BY date, dau, wau, mau""".format(events_table, start_date, where_clause), ["date", "dau", "wau", "mau", "weekly_engagement", "monthly_engagement"]
