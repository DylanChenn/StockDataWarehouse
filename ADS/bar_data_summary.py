import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils


def summarize(spark_sess, date):
    codes = utils.getCode().split(',')
    for i, code in enumerate(codes):
        codes[i] = code[2:] + '.' + code[0:2].upper()
    target_time_bar_intervals = ["1m", "5m"]

    base_sql_time = utils.readSQL('./ADS/summarize_timebar.sql')
    base_sql_day = utils.readSQL('./ADS/summarize_daybar.sql')
    for code, i in enumerate(codes):
        print(f"{i+1}/{len(codes)}\tProcessing {code}....")
        for interval in target_time_bar_intervals:
            sql = base_sql_time.format(interval, code, date, interval, code, date, interval, code, date, interval, code, date)
            spark_sess.sql(sql)
        sql = base_sql_day.format(code, code, date, code, date, code, date)
        spark_sess.sql(sql)


if __name__ == '__main__':
    cur_date = utils.getDate()
    spark_sess = utils.init_spark_session("yarn", "summarize_bar_data")
    print("Start summarizing bar data from different tables...")
    summarize(spark_sess, cur_date)
