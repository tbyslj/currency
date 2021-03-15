# -*- coding: utf-8 -*-
"""
@File: mongodb
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import os
import json
from gli.logger import Logger
import pymongo

class MongoDB(object):
    def __init__(self, log=Logger(folder="mongodb")):
        """
        建立mongo连接
        """
        self.logger = log
        self.client = {}
        module_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # load profile
        mongo_file = os.path.join(module_path, "config", "mongo.json")
        with open(mongo_file, "r", encoding="utf-8-sig") as file:
            mongo_config = json.load(file)
            for name in mongo_config:
                self.client[name] = pymongo.MongoClient(**mongo_config[name])

    def __enter__(self):
        return self

    def __exit__(self, type, value, trace):
        if type is not None:
            self.logger.error("line %s. %s" % (trace.tb_lineno, value))
        self.close()

    def close(self):
        """
        mongo断开连接
        """
        try:
            for name in self.client.keys():
                self.client[name].close()
        except Exception as err:
            self.logger.error(err)