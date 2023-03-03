# -*-coding:utf-8-*-
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils
import datetime
import time
import _thread


basedir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def getDataAndSend(producer, codes):
    data = utils.getDataFromAPI(codes)
    utils.kafkaSend(producer, 'stock_raw', 'TikData', data)


if __name__ == '__main__':
    codes = utils.getCode()
    print(codes)
    producer = utils.kafkaProducerInitialization("node1:9092")
    print("kafkaConnection started")
    startTime1 = datetime.time(9, 25)
    startTime2 = datetime.time(13)
    endTime1 = datetime.time(11, 30, 2)
    endTime2 = datetime.time(15, 00, 5)

    while True:
        dt = datetime.datetime.now().time()
        if startTime1 <= dt <= endTime1 or startTime2 <= dt <= endTime2:
            _thread.start_new_thread(getDataAndSend, (producer, codes))
            time.sleep(0.08)
