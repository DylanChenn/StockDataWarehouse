insert into table dwd.tikdata partition (code='{}', dt='{}')
select
    name,open,high,low,price,
    volume - lag(volume, 1, 0) over (partition by code order by time) as vol,
    amont - lag(volume, 1, 0) over (partition by code order by time) as amt,
    bid1_volume,bid1_price,bid2_volume,bid2_price,
    bid3_volume,bid3_price,bid4_volume,bid4_price,bid5_volume,bid5_price,
    ask1_volume,ask1_price,ask2_volume,ask2_price,
    ask3_volume,ask3_price,ask4_volume,ask4_price,ask5_volume,ask5_price,
    time
from
    ods.stockrawdata
where
    code='{}' and dt='{}'
order by
    time