# -*-coding:utf-8-*-
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from Utils import utils
import json


def checkIfOdsDone(sps, date):
    sql = "select count(*) as count from ods.stockrawdata where date=\'{}\' and time>=\'15:00:00\'".format(date)
    res = sps.sql(sql)
    count = int(res.select("count").first()[0])
    print(count)
    return count > 0


def processTikData(sps, date):
    codes = utils.getCode().split(',')
    base_sql = utils.readSQL("./DWD/ods_to_dwd.sql")
    for i, code in enumerate(codes):
        code = code[2:] + '.' + code[0:2].upper()
        print("{}/{}: Processing stock {}...".format(i + 1, len(codes), code))
        sql = base_sql.format(code, date, code, date)
        sps.sql(sql)


if __name__ == '__main__':
    date = utils.getDate()
    sparkSess = utils.init_spark_session("yarn", "tick_generation", exec_cores = 2, num_exec = 3, exec_memory = 2, parallelism = 12)
    print("Checking if storing raw data in ODS is done...")
    # while not checkIfOdsDone(sparkSess, date):
    #     time.sleep(1)
    # time.sleep(30)
    print("ODS is done, start processing...")
    processTikData(sparkSess, date)
