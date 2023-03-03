INSERT INTO
    ads.minbar_{}_summary
PARTITION
    (code='{}' and dt='{}')
SELECT
    t1.bar_time
    t1.open,
    t1.high,
    t1.low,
    t1.close,
    t1.volume,
    t1.amount,
    t2.ma5,
    t2.ma10,
    t2.ma20,
    macd
FROM
    (SELECT * FROM dwb.minbar_{} WHERE code='{}' and dt='{}') t1
    LEFT JOIN (SELECT * FROM dws.indicator_MA_time_{} WHERE code='{}' and dt='{}') t2 on t1.bar_time=t2.bar_time
    LEFT JOIN (SELECT * FROM dws.indicator_MACD_time_{} WHERE code='{}' and dt='{}') t3 on t2.bartime=t3.bar_time
