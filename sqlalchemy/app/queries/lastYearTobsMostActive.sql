SELECT
    date,
    station,
    tobs
FROM
    measurement
WHERE
    date >= (
                SELECT
                    date(MAX(date), '-365 day')
                FROM
                    measurement
            )
    AND
    station = (
                SELECT
                    s.station
                FROM
                    station s
                JOIN measurement m on s.station = m.station
                GROUP BY s.station
                ORDER BY
                    count(*) desc
                LIMIT 1
            )
ORDER BY
    date, station