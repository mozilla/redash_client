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
    ORDER BY date, week_number""".format(start_date, experiment_id, addon_versions), ["date", "event_rate", "type"]
