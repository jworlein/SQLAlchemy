SELECT
    date,
    prcp
FROM
    measurement
WHERE
    date >= (
                SELECT
                   date(MAX(date), '-365 day')
                FROM
                    measurement
            )
ORDER BY
    date