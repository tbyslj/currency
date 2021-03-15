# -*- coding: utf-8 -*-
"""
@File: logger
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import os
import json
import logging
import logging.config

class Logger(object):
    """创建日志器"""

    def __new__(cls, logname: str = "log_debug", folder: str = "logs"):
        """
        创建日志
        :param logname: 日志名称
        :param folder: 存放日志目录
        :return 返回一个日志器
        """
        # 获取当前文件所在路径
        module_path = os.path.dirname(os.path.realpath(__file__))
        if folder == "logs":
            logger_path = os.path.join(module_path, folder)
        else:
            logger_path = os.path.join(module_path, "logs", folder)
        # 创建目录
        if not os.path.exists(logger_path):
            os.makedirs(logger_path)

        # 拼接配置文件所在路径
        logger_file = os.path.join(module_path, "config", "logger.json")

        # 获取配置信息
        with open(logger_file, "r", encoding="utf-8-sig") as file:
            logger_config = json.load(file)
            logger_config["handlers"]["file"]["filename"] = os.path.join(
                logger_path, logger_config["handlers"]["file"]["filename"])
            logging.config.dictConfig(logger_config)
        return logging.getLogger(logname)