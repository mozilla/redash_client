def event_rate(event, start_date, experiment_id):
  return """
    WITH control_events_by_day AS
      (SELECT date, COUNT(DISTINCT session_id)
      FROM activity_stream_events_daily
      WHERE event = '{0}'
      AND date >= '{1}'
      AND (experiment_id = 'n/a' OR experiment_id IS NULL)
      GROUP BY date
      ORDER BY date),

    control_session_counts_per_day AS
      (SELECT date, count(DISTINCT session_id)
      FROM activity_stream_stats_daily AS stats
      WHERE date >= '{1}'
      AND stats.session_id IS NOT NULL
      AND stats.session_id <> 'n/a'
      AND (experiment_id = 'n/a' OR experiment_id IS NULL)
      GROUP BY date
      ORDER BY date),

    experiment_clicks_by_day AS
      (SELECT date, COUNT(DISTINCT session_id)
      FROM activity_stream_events_daily
      WHERE event = '{0}'
      AND date >= '{1}'
      AND experiment_id = '{2}'
      GROUP BY date
      ORDER BY date),

    experiment_session_counts_per_day AS
      (SELECT date, count(DISTINCT session_id)
      FROM activity_stream_stats_daily AS stats
      WHERE date >= '{1}'
      AND experiment_id = '{2}'
      AND stats.session_id IS NOT NULL
      AND stats.session_id <> 'n/a'
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
    ORDER by a.date)""".format(event, start_date, experiment_id), ["date", "event_rate", "type"]


