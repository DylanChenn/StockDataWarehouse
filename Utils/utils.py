from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json
import requests
import pyspark
import datetime
import os

basedir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def getCode():
    f = open(basedir + r"/Config/StockSubscribedList.csv")
    lines = f.readlines()
    codes = lines[0][:-1]
    return codes


def getDataFromAPI(code: str):
    headers = {'referer': 'http://finance.sina.com.cn'}
    response = requests.get('http://hq.sinajs.cn/list=' + code, headers=headers, timeout=6)
    data = response.text
    return data


def kafkaProducerInitialization(host: str):
    producer = KafkaProducer(
        bootstrap_servers=[host],
        key_serializer=lambda k: json.dumps(k).encode(),
        value_serializer=lambda v: json.dumps(v).encode())
    return producer


def kafkaSend(producer: KafkaProducer, topic: str, key: str, data: str):
    future = producer.send(
        topic=topic,
        key=key,
        value=data)
    # try:
    #     future.get(timeout=10)  # 监控是否发送成功
    # except kafka_errors:  # 发送失败抛出kafka_errors
    #     traceback.format_exc()


def init_kafka_consumer(host: str, topic: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=host
    )


def init_spark_session(master: str, appName: str, deployMode: str = "cluster", exec_cores: int = 1, num_exec: int = 1,
                       exec_memory: int = 1, parallelism: int = 10):
    session = pyspark.sql.SparkSession.builder \
        .master(master) \
        .appName(appName) \
        .config("spark.executor.cores", str(exec_cores)) \
        .config("spark.executor.instances", str(num_exec)) \
        .config("spark.executor.memory", str(exec_memory) + "G") \
        .config("spark.default.parallelism", str(parallelism)) \
        .enableHiveSupport().getOrCreate()
    return session


def readSQL(path: str):
    f = open(path)
    return f.read()


def readJson(path: str):
    with open(path) as f:
        return json.load(f)


def getDate():
    return datetime.datetime.now().strftime("%Y-%m-%d")
