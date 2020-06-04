#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import json
import logging
import os
import socket
import random
import string
from kafka.consumer import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
from time import sleep
import traceback

# consumer配置
BOOTSTRAP_IP = "172.17.0.2:9092"
DEFAULT_PORT = ""
TOPIC = "test"
LOGGING_LEVEL = logging.DEBUG

# 日志配置
logger_path = os.getcwd()
logger_name = 'consumer_log'
logger_level = logging.DEBUG

# 启用日志
logger = BasicLog(logger_path=logger_path, logger_name=logger_name, logger_level=logger_level).get_log()

# 消费者基类
# 修改msg_handler进行消息的进一步处理
# 修改callback_handler进行消息处理完后的回调处理
class BasicConsuemr():
    
    def __init__(self):
        self.thread_pool = ThreadPoolExecutor(10)

    # 随机生成消费组名称
    def random_cgroup(self):
        salt = ''.join(random.sample(string.ascii_letters + string.digits, 12))
        return salt

    def receive_message(self, cgroup_name):
        consumer = KafkaConsumer(TOPIC,
                                group_id=cgroup_name,
                                bootstrap_servers=[BOOTSTRAP_IP])
        try:
            for msg in consumer:
                msg = msg.value
                logger.info("consumer receive message %s" % msg)
                future = self.thread_pool.submit(self.msg_handler, (msg))
                future.add_done_callback(self.callback_handler)
        except Exception:
            logger.error("consumer error")
            logger.error(traceback.format_exc())
        finally:
            self.thread_pool.shutdown(wait=True)
    
    def msg_handler(self, msg):
        sleep(5)
        logger.debug("handler success")
        return 1
    
    def callback_handler(self, future):
        sleep(2)
        logger.debug("callback success %s" % future.result())

if __name__ == '__main__':
    consumer = ConsuemrR()
    cgroup_name = consumer.random_cgroup()
    consumer.receive_message(cgroup_name)