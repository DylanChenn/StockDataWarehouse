insert into table
    dwb.daybar
partition
    (code='{}')
select
    first(dt),
    first(price) as open,
    avg(high),
    avg(low),
    last(price) as close,
    sum(volume) as vol,
    sum(amont) as amt
from
    dwd.tikdata
where
    code='{}' and dt='{}'