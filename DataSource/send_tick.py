# -*-coding:utf-8-*-
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils
from Utils.stock_tick import StockTick
import json


def GetAndSend():
    consumer = utils.init_kafka_consumer('node1:9092', 'stock_raw')
    producer = utils.kafkaProducerInitialization('node1:9092')

    print("KafkaConnection Started.")
    last_vol = {}
    for message in consumer:
        lines = json.loads(message.value.decode()).split('\n')

        for line in lines:
            if not line:
                continue
            tick = extract_info(line)
            if tick.code not in last_vol or last_vol[tick.code] != tick.accum_volume:
                # print(tick.to_str())
                utils.kafkaSend(producer, 'queue2ods', 'Tik_data', tick.to_str())
            last_vol[tick.code] = tick.accum_volume


def extract_info(line):
    line_cols = line.split('=')
    stock_code = line_cols[0].split('_')[-1]
    stock_code = stock_code[2:] + '.' + stock_code[0:2].upper()  # sh600001 => 600001.SH
    cols = line_cols[1].replace("\"", "").split(',')
    tick = StockTick(stock_code, *cols[0:6], *cols[8:32])
    return tick


if __name__ == '__main__':
    GetAndSend()
