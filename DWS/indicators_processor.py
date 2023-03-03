import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils


def generate_indicators(spark_sess, date):
    codes = utils.getCode().split(',')
    for i, code in enumerate(codes):
        codes[i] = code[2:] + '.' + code[0:2].upper()
    target_time_bar_intervals = ["1m", "5m"]
    indicators_all = os.listdir('./DWS/indicator_sql/')
    indicators = []
    for ind in indicators_all:
        ind = ind.split('_')[0]
        if ind not in indicators:
            indicators.append(ind)

    total_count = len(indicators)*len(codes)*(len(target_time_bar_intervals)+1)
    cur = 0
    for indicator_name in indicators:
        base_sql_time = utils.readSQL('./DWS/indicator_sql/' + indicator_name + '_time.sql')
        base_sql_day = utils.readSQL('./DWS/indicator_sql/' + indicator_name + '_day.sql')
        for code in codes:
            for interval in target_time_bar_intervals:
                cur += 1
                print(f"{cur}/{total_count}\tProcessing {code} in {indicator_name}_time_{interval}....")
                sql = base_sql_time.format(indicator_name + "_time_" + interval, code, date, interval, code, date)
                spark_sess.sql(sql)
            cur += 1
            print(f"{cur}/{total_count}\tProcessing {code} in {indicator_name}_day....")
            sql = base_sql_day.format(indicator_name + "_day", code, code)
            spark_sess.sql(sql)


if __name__ == '__main__':
    cur_date = utils.getDate()
    spark_sess = utils.init_spark_session("yarn", "indicators_generation")
    print("Start generating indicators from bar data...")
    generate_indicators(spark_sess, cur_date)
