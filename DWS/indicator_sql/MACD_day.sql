INSERT INTO TABLE
    dws.indicator_{}
partition
    (code='{}')
WITH ema_cte AS (
  SELECT
      code,
      dt,
      AVG(close) OVER (partition by code ORDER BY dt ROWS BETWEEN (12 - 1) PRECEDING AND CURRENT ROW) AS ema_short,
      AVG(close) OVER (partition by code ORDER BY dt ROWS BETWEEN (26 - 1) PRECEDING AND CURRENT ROW) AS ema_long
  FROM
      dwb.daybar
  WHERE
      code='{}'
),
dea_cte AS (
  SELECT
      dt,
      (ema_short - ema_long) AS diff,
      AVG((ema_short - ema_long)) OVER (partition by code ORDER BY dt ROWS BETWEEN (9 - 1) PRECEDING AND CURRENT ROW) AS dea
  FROM ema_cte
)
SELECT
    dt,
    diff - dea AS macd
FROM dea_cte
