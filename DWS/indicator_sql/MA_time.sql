insert into table 
    dws.indicator_{} 
partition 
    (code='{}', dt='{}')
select
    bar_time,
    avg(close) over (partition by code order by bar_time rows between (5 - 1) preceding and current row) as ma5,
    avg(close) over (partition by code order by bar_time rows between (10 - 1) preceding and current row) as ma10,
    avg(close) over (partition by code order by bar_time rows between (20 - 1) preceding and current row) as ma20
from
    dwb.minbar_{}
where
    code='{}' and dt='{}'