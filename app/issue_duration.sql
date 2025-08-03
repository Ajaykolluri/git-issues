SELECT
    strftime('%Y-%m', closed_at) AS year_month,
    CEIL(AVG( (strftime('%s', closed_at) - strftime('%s', created_at)) / 3600.0 )) AS avg_closing_hours,
    CEIL(AVG( (julianday(closed_at) - julianday(created_at)) )) AS avg_closing_days
FROM issues
WHERE closed_at IS NOT NULL
GROUP BY year_month
ORDER BY year_month;