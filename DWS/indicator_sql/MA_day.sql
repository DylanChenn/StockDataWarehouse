insert into table
    dws.indicator_{}
partition
    (code='{}')
select
    dt,
    avg(close) over (partition by code order by dt rows between (5 - 1) preceding and current row) as ma5,
    avg(close) over (partition by code order by dt rows between (10 - 1) preceding and current row) as ma10,
    avg(close) over (partition by code order by dt rows between (20 - 1) preceding and current row) as ma20
from
    dwb.daybar
where
    code='{}'