INSERT INTO
    ads.daybar_summary
PARTITION
    (code='{}')
SELECT
    t1.dt,
    t1.open,
    t1.high,
    t1.low,
    t1.close,
    t1.volumn,
    t1.amount,
    t2.ma5,
    t2.ma10,
    t2.ma20,
    macd
FROM
    (SELECT * FROM dwb.daybar WHERE code='{}' and dt='{}') t1
    LEFT JOIN (SELECT * FROM dws.indicator_MA_day WHERE code='{}' and dt='{}') t2 on t1.dt=t2.dt
    LEFT JOIN (SELECT * FROM dws.indicator_MACD_day WHERE code='{}' and dt='{}') t3 on t2.dt=t3.dt

