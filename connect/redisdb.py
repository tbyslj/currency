# -*- coding: utf-8 -*-
"""
@File: mongodb
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import os
import json
from redis import StrictRedis
from gli.logger import Logger

class RedisDB(object):
    def __init__(self):
        """
        建立redis连接
        """
        self.logger = Logger(folder="redis")
        module_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # load profile
        redis_file = os.path.join(module_path, "config", "redis.json")
        config = json.load(open(redis_file, "r", encoding="utf-8-sig"))
        redis_config = config["local_rw"]
        self.client = StrictRedis(host=redis_config["host"], port=redis_config["port"],
                                  password=redis_config["password"], decode_responses=True)

    def addset(self, db, fp):
        """
        添加
        :param db: 数据库
        :param fp: 添加值
        :return: bool结果
        """
        return self.client.sadd(db, fp)


