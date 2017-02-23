def event_rate(event, start_date, experiment_id, addon_versions):
  return """
    WITH control_events_by_day AS
      (SELECT date, COUNT(DISTINCT session_id)
      FROM activity_stream_events_daily
      WHERE event IN ({0})
      AND date >= '{1}'
      AND (experiment_id = 'n/a' OR experiment_id IS NULL)
      AND addon_version IN ({3})
      GROUP BY date
      ORDER BY date),

    control_session_counts_per_day AS
      (SELECT date, count(DISTINCT session_id)
      FROM activity_stream_stats_daily AS stats
      WHERE date >= '{1}'
      AND stats.session_id IS NOT NULL
      AND stats.session_id <> 'n/a'
      AND (experiment_id = 'n/a' OR experiment_id IS NULL)
      AND addon_version IN ({3})
      GROUP BY date
      ORDER BY date),

    experiment_clicks_by_day AS
      (SELECT date, COUNT(DISTINCT session_id)
      FROM activity_stream_events_daily
      WHERE event IN ({0})
      AND date >= '{1}'
      AND experiment_id = '{2}'
      AND addon_version IN ({3})
      GROUP BY date
      ORDER BY date),

    experiment_session_counts_per_day AS
      (SELECT date, count(DISTINCT session_id)
      FROM activity_stream_stats_daily AS stats
      WHERE date >= '{1}'
      AND experiment_id = '{2}'
      AND stats.session_id IS NOT NULL
      AND stats.session_id <> 'n/a'
      AND addon_version IN ({3})
      GROUP BY date
      ORDER BY date)

    (SELECT a.date, 'experiment' AS type, COALESCE(b.count, 0) / a.count::float * 100 AS event_rate
    FROM experiment_session_counts_per_day AS a
    LEFT JOIN experiment_clicks_by_day AS b
    ON a.date = b.date
    ORDER by a.date)

    UNION ALL

    (SELECT a.date, 'control' AS type, COALESCE(b.count, 0) / a.count::float * 100 AS event_rate
    FROM control_session_counts_per_day AS a
    LEFT JOIN control_events_by_day AS b
    ON a.date = b.date
    ORDER by a.date)""".format(event, start_date, experiment_id, addon_versions), ["date", "event_rate", "type"]

def disable_rate(start_date, experiment_id, addon_versions):
  return """
    WITH exp_disabled AS
      (SELECT date, COUNT(DISTINCT client_id)
      FROM activity_stream_events_daily
      WHERE date >= {0}
      AND (event = 'uninstall' OR event = 'disable')
      AND experiment_id = '{1}'
      AND addon_version IN ({2})
      GROUP BY date
      ORDER BY date),

    control_disabled AS
      (SELECT date, COUNT(DISTINCT client_id)
      FROM activity_stream_events_daily
      WHERE date >= {0}
      AND (event = 'uninstall' OR event = 'disable')
      AND (experiment_id = 'n/a' OR experiment_id IS NULL)
      AND addon_version IN ({2})
      GROUP BY date
      ORDER BY date),

    exp_users AS
      (SELECT date, COUNT(DISTINCT client_id)
      FROM activity_stream_stats_daily
      WHERE date >= {0}
      AND experiment_id = '{1}'
      AND addon_version IN ({2})
      GROUP BY date
      ORDER BY date),

    control_users AS
      (SELECT date, COUNT(DISTINCT client_id)
      FROM activity_stream_stats_daily
      WHERE date >= {0}
      AND (experiment_id = 'n/a' OR experiment_id IS NULL)
      AND addon_version IN ({2})
      GROUP BY date
      ORDER BY date)

    (SELECT exp_disabled.date, 'experiment' AS type, exp_disabled.count / exp_users.count::float * 100 AS disable_rate
    FROM exp_users
    LEFT JOIN exp_disabled
    ON exp_disabled.date = exp_users.date
    ORDER BY date)

    UNION ALL

    (SELECT control_disabled.date, 'control' AS type, control_disabled.count / control_users.count::float * 100 AS disable_rate
    FROM control_users
    LEFT JOIN control_disabled
    ON control_disabled.date = control_users.date
    ORDER BY date)
  """.format(start_date, experiment_id, addon_versions), ["date", "disable_rate", "type"]

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

def active_users(events_table, start_date, where_clause):
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

def retention_diff(start_date, experiment_id, addon_versions):
  return """
    WITH control_interactions AS
      (SELECT client_id, date
      FROM activity_stream_events_daily
      WHERE (experiment_id = 'n/a' OR experiment_id IS NULL)
      AND addon_version IN ({2})
      ORDER BY date),

    exp_interactions AS
      (SELECT client_id, date
      FROM activity_stream_events_daily
      WHERE experiment_id = '{1}'
      AND addon_version IN ({2})
      ORDER BY date),

    control_cohort_count AS
      (SELECT DATE_TRUNC('day', stats.date) AS date, COUNT(DISTINCT client_id)
      FROM activity_stream_events_daily AS stats
      WHERE (experiment_id = 'n/a' OR experiment_id IS NULL)
      AND addon_version IN ({2})
      GROUP BY 1),

    exp_cohort_count AS
      (SELECT DATE_TRUNC('day', stats.date) AS date, COUNT(DISTINCT client_id)
      FROM activity_stream_events_daily AS stats
      WHERE experiment_id = '{1}'
      AND addon_version IN ({2})
      GROUP BY 1),

    control_retention AS
      (select * from (select date, period as week_number, retained_users as value, new_users as total,
      max(period) over (PARTITION BY date) AS max_week_num
      from (
        select
          DATE_TRUNC('day', anow.date) AS date,
          date_diff('day', DATE_TRUNC('day', anow.date), DATE_TRUNC('day', athen.date)) as period,
          max(cohort_size.count) as new_users,
          count(distinct anow.client_id) as retained_users,
          count(distinct anow.client_id) /
            max(cohort_size.count)::float as retention
        from control_interactions anow
        left join control_interactions as athen on
          anow.client_id = athen.client_id
          and anow.date <= athen.date
        left join control_cohort_count as cohort_size on
          anow.date = cohort_size.date
        group by 1, 2) t
      where period is not null
      and date >= '{0}'
      order by date, period)
      where week_number < max_week_num),

    exp_retention AS
      (select * from (select date, period as week_number, retained_users as value, new_users as total,
      max(period) over (PARTITION BY date) AS max_week_num
      from (
        select
          DATE_TRUNC('day', anow.date) AS date,
          date_diff('day', DATE_TRUNC('day', anow.date), DATE_TRUNC('day', athen.date)) as period,
          max(cohort_size.count) as new_users,
          count(distinct anow.client_id) as retained_users,
          count(distinct anow.client_id) /
            max(cohort_size.count)::float as retention
        from exp_interactions anow
        left join exp_interactions as athen on
          anow.client_id = athen.client_id
          and anow.date <= athen.date
        left join exp_cohort_count as cohort_size on
          anow.date = cohort_size.date
        group by 1, 2) t
      where period is not null
      and date >= '{0}'
      order by date, period)
      where week_number < max_week_num)

    SELECT con.date, con.week_number, exp.value as c, exp.total as d, con.value as e, con.total as f, ((exp.value*con.total) - (con.value*exp.total)) AS value, con.total * exp.total AS total
    FROM control_retention AS con
    LEFT JOIN exp_retention AS exp
    ON con.date = exp.date
    AND con.week_number = exp.week_number
    ORDER BY date, week_number""".format(start_date, experiment_id, addon_versions), []
