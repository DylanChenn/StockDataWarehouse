import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils


def createBarFromTik(spark_sess, curdate):
    codes = utils.getCode().split(',')
    for i, code in enumerate(codes):
        codes[i] = code[2:] + '.' + code[0:2].upper()
    intervals = [60, 300, 600, 1200]
    total = (len(intervals) + 1) * len(codes)
    cur_cnt = 1
    base_sql_time_bar = utils.readSQL("./DWB/tick_to_time_bar.sql")
    base_sql_day_bar = utils.readSQL("./DWB/tick_to_day_bar.sql")

    # time bar generating, different intervals
    for interval in intervals:
        for code in codes:
            print(f"{cur_cnt}/{total}\tProcessing {code} in {int(interval / 60)} min bar....")
            cur_cnt += 1
            sql = base_sql_time_bar.format(int(interval / 60), code, curdate, interval, code, curdate)
            spark_sess.sql(sql)

    # day bar generating
    for code in codes:
        print(f"{cur_cnt}/{total}\tProcessing {code} in day bar....")
        cur_cnt += 1
        sql = base_sql_day_bar.format(code, code, curdate)
        spark_sess.sql(sql)


if __name__ == '__main__':
    date = utils.getDate()
    spark_sess = utils.init_spark_session("yarn", "bar_generation")
    print("Start creating bar data from tik...")
    createBarFromTik(spark_sess, date)
