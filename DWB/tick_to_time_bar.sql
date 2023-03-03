insert into table
    dwb.minbar_{}m
partition
    (code='{}', dt='{}')
select
    from_unixtime(unix_timestamp(concat(dt, ' ', time), 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(concat(dt, ' ', time), 'yyyy-MM-dd HH:mm:ss') % {}) as bar_time,
    first(price) as open,
    max(price) as high,
    min(price) as low,
    last(price) as close,
    sum(volume) as vol,
    sum(amont) as amt
from
    dwd.tikdata
where
    code='{}' and dt='{}'
group by
    bar_time
order by
    bar_time