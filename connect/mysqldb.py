# -*- coding: utf-8 -*-
"""
@File: mysql
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import os
import json
import pymysql
from gli.logger import Logger

class MysqlDB(object):
    def __init__(self, name, database):
        """
        建立mysql连接
        """
        self.logger = Logger(folder="mysql")
        self.client = {}
        module_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        # load profile
        mysql_file = os.path.join(module_path, "config", "mysql.json")
        config = json.load(open(mysql_file, "r", encoding="utf-8-sig"))
        self.client = pymysql.connect(**config[name], database=database)
        self.cursor = self.client.cursor()

    def add(self, table: str, data: dict):
        """
        增加
        :param table: 表名
        :param data: 插入数据 {"name": "tby", "age": "20"}
        :return: 成功或失败
        """
        try:
            values = tuple(data.values())
            value_len = self.seek(table)
            if value_len != len(data):
                return False, "数据字段不匹配"
            sql = "INSERT INTO {table} VALUES {values}".format(table=table, values=values)
            try:
                self.cursor.execute(sql)
                self.client.commit()
                return True, "插入成功"
            except Exception as e:
                self.logger.info(e)
                self.client.rollback()
        except Exception as e:
            self.logger.error(e)

    def seek(self, table: str, condition):
        """
        查询
        :param table: 表名
        :param condition: 查询条件
        :return:
        """
        keys = tuple()
        try:
            if condition:
                sql = "select * from %s%s%s" % (table, " ", condition)
                try:
                    self.cursor.execute(sql)
                    keys = self.cursor.description
                    self.client.commit()
                    res = self.cursor.fetchall()
                    return res, keys
                except Exception as e:
                    self.logger.info(e)
                    self.client.rollback()
            else:
                sql = 'SELECT * FROM %s' % table
                try:
                    self.cursor.execute(sql)
                    self.client.commit()
                    res = self.cursor.fetchall()
                    return res, keys
                except Exception as e:
                    self.logger.info(e)
                    self.client.rollback()
        except Exception as e:
            self.logger.error(e)

    def update(self, table: str, datas: dict, conditions: dict=None):
        """
        修改
        :param table: 表名
        :param datas: 需要修改的字段 {"age": "20"}
        :param conditions: 查询条件 {"age": ">21"}
        :return: bool
        """
        try:
            data = ""
            condition = ""
            for k, v in datas.items():
                data += k + "=" + v
            if conditions:
                for k, v in conditions.items():
                    condition += k + v
            if condition:
                sql = "UPDATE {table} SET {data} WHERE {condition}".format(table=table, data=data, condition=condition)
            else:
                sql = "UPDATE {table} SET {data}".format(table=table, data=data)
            try:
                self.cursor.execute(sql)
                self.client.commit()
                return True
            except Exception as e:
                self.logger.info(e)
                self.client.rollback()
                return False
        except Exception as e:
            self.logger.error(e)

    def delete(self, table: str, conditions: dict):
        """
        删除
        :param table: 表名
        :param conditions: 查询条件 查询条件 {"age": "=21"}
        :return:
        """
        try:
            condition = ""
            for k, v in conditions.items():
                condition += k + v
            sql = "DELETE FROM {table} WHERE {condition}".format(table=table, condition=condition)
            try:
                self.cursor.execute(sql)
                self.client.commit()
                return True
            except Exception as e:
                self.logger.info(e)
                self.client.rollback()
                return False
        except Exception as e:
            self.logger.error(e)

    def close(self):
        """
        关闭连接
        :return:
        """
        try:
            self.cursor.close()
            self.client.close()
        except Exception as e:
            self.logger.error(e)

if __name__ == '__main__':
    my_mysql = MysqlDB("tby")
    res = my_mysql.delete("user", {"age": "30"})
    print(res)
    my_mysql.close()

