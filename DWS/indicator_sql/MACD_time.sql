INSERT INTO TABLE
    dws.indicator_{}
partition
    (code='{}', dt='{}')
WITH ema_cte AS (
  SELECT
      code,
      bar_time,
      AVG(close) OVER (partition by code ORDER BY bar_time ROWS BETWEEN (12 - 1) PRECEDING AND CURRENT ROW) AS ema_short,
      AVG(close) OVER (partition by code ORDER BY bar_time ROWS BETWEEN (26 - 1) PRECEDING AND CURRENT ROW) AS ema_long
  FROM
      dwb.minbar_{}
  WHERE
      code='{}' and dt='{}'
),
dea_cte AS (
  SELECT
      bar_time,
      (ema_short - ema_long) AS diff,
      AVG((ema_short - ema_long)) OVER (partition by code ORDER BY bar_time ROWS BETWEEN (9 - 1) PRECEDING AND CURRENT ROW) AS dea
  FROM ema_cte
)
SELECT
    bar_time,
    diff - dea AS macd
FROM dea_cte
