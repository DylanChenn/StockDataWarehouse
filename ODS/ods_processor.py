# -*-coding:utf-8-*-
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils
from Utils.stock_tick import StockTick
import json


def generate_ods_data():
    consumer = utils.init_kafka_consumer('node1:9092', 'queue2ods')
    print("KafkaConnection Started.")
    spark_sess = utils.init_spark_session("yarn", "ods_storing", exec_cores = 1, num_exec = 1)
    print("SparkSession Started.")

    flag = {}
    batch_stock = {}   # buffer to temporarily storing the tick data
    date = ""
    base_sql = utils.readSQL("./ODS/insert_to_ods.sql")
    max_records_num = 120
    max_time_interval = 120   # seconds
    while True:
        messages = consumer.poll(1000)
        for key in messages:
            for message in messages[key]:
                value = json.loads(message.value.decode())
                tick = StockTick(*value.split(','))
                tick.process_for_sql()
                date = tick.data[-2]
                if tick.code not in flag:
                    flag[tick.code] = [0, time.time()]
                    batch_stock[tick.code] = [tick.code, date, []]
                flag[tick.code][0] += 1
                batch_stock[tick.code][2].append("(" + ','.join(tick.data[1:-2]) + "," + tick.data[-1] + ")")
        for code in flag:
            if flag[code][0] >= max_records_num or time.time() - flag[code][1] >= max_time_interval:
                if len(batch_stock[code][2]) > 0:
                    print(f"Storing {code} with {len(batch_stock[code][2])} records.")
                    sql = base_sql.format(batch_stock[code][0], batch_stock[code][1], ','.join(batch_stock[code][2]))
                    spark_sess.sql(sql)
                flag[code] = [0, time.time()]
                batch_stock[code] = [code, date, []]


if __name__ == '__main__':
    generate_ods_data()
