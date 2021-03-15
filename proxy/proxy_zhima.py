# -*- coding: utf-8 -*-
"""
@File: logger
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import datetime
import json
import re
import time
from datetime import datetime, timedelta
from logger import Logger
from mongodb import MongoDB
from http_request import HttpRequest


class ProxyZhima(HttpRequest):
    """Zhima Proxy
    """
    def __init__(self, retry: int = 3, timeout: int = 5):
        super().__init__(retry, timeout)
        self.logger = Logger(folder="zhima")
        self.url = "http://webapi.http.zhimacangku.com/getip?num=1&type=2&pro=&city=0&yys=0&port=11&time=1&ts=1&ys=0&cs=1&lb=1&sb=0&pb=4&mr=1&regions="
        self.white = "http://web.http.cnapi.cc/index/index/save_white?neek=80313&appkey=1745838ce83ef74c512a3d200585c1b4&white="
        client = MongoDB()
        if "local_rw" in client:
            self.reader = client["local_rw"]["proxies"]
        else:
            raise RuntimeError("The specified configuration item could not be found.")
        if "local_rw" in client:
            self.writer = client["local_rw"]["proxies"]
        else:
            raise RuntimeError("The specified configuration item could not be found.")

    def add_white(self, ip: str):
        """Add IP white list.
        Args:
            ip: IP address.
        Returns:
            Success returns True, otherwise False.
        """
        rst = False
        try:
            # Send GET Request.
            response = super().get(self.white + ip)
            if response:
                data = json.loads(response.text)
                if "success" in data and data["success"]:
                    self.logger.info("Add %s to IP white list successfully." % ip)
                    rst = True
                else:
                    self.logger.info("Failed to add %s to IP white list." % ip)
        except Exception as err:
            self.logger.error(err)
        finally:
            return rst

    def get_ip(self):
        """Get proxy IP.
        Returns:
            Success returns True, otherwise False.
        """
        rst = False
        try:
            # Send GET Request.
            response = super().get(self.url)
            if response:
                data = json.loads(response.text)
                # If successful.
                if "success" in data and data["success"]:
                    for item in data["data"]:
                        self.writer["proxy"].insert_one({
                            "ip":          item["ip"],
                            "port":        item["port"],
                            "expire_time": datetime.strptime(item["expire_time"], "%Y-%m-%d %H:%M:%S"),
                            "city":        item["city"],
                            "used":        0,
                            "fail":        0,
                            "create_time": datetime.now(),
                            "update_time": datetime.now() })
                        self.logger.info("%s\t%s:%s\t%s" % (item["city"], item["ip"], item["port"], item["expire_time"]))
                    rst = True
                # Add IP white list.
                elif "code" in data and data["code"] == 113 and "msg" in data:
                    pattern = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})")
                    list_ip = re.findall(pattern, data["msg"])
                    if list_ip:
                        # Add IP white list.
                        if self.add_white(list_ip[0]):
                            # "Get proxy IP.
                            rst = self.get_ip()
        except Exception as err:
            self.logger.error(err)
        finally:
            return rst

    def is_alive(self, interval:int):
        """Check if the spider is alive.
        Args:
            interval: Maximum interval of last update.
        Returns:
            Success returns True, otherwise False.
        """
        rst = False
        try:
            # Modify update time when spider gets IP address.
            doc = self.reader["proxy"].find_one(
                { "update_time": {"$gt": datetime.now() - timedelta(seconds=interval)}},
                { "ip": 1 })
            if doc is None:
                # No data in database at initial state.
                doc = self.reader["proxy"].find_one({}, { "ip": 1 })
                if doc is None:
                    rst = True
            else:
                rst = True
        except Exception as err:
            self.logger.error(err)
        finally:
            return rst

    def get_count(self, margin:int):
        """Get the number of available IPs.
        Args:
            margin: Expiration time margin of proxy IP.
        Returns:
            Success returns the number of available proxy IPs, otherwise 0.
        """
        rst = 0
        try:
            cursor = self.reader["proxy"].find(
                { "expire_time": {"$gt": datetime.now() + timedelta(seconds=margin)}},
                { "ip": 1 })
            rst = len([doc["ip"] for doc in cursor])
        except Exception as err:
            self.logger.error(err)
        finally:
            return rst

    def run(self, count: int = 1, interval: int = 10, margin: int = 10):
        """Maintain a specified number of available IP addresses.
        Args:
            count: Number of available IP addresses.
            interval: Maximum interval of last update.
            margin: Expiration time margin of proxy IP.
        """
        while True:
            try:
                if self.is_alive(interval):
                    if self.get_count(margin) < count:
                        if self.get_ip():
                            # Clear spider usage records.
                            self.writer["proxy"].update_many(
                                { "expire_time": {"$gt": datetime.now() + timedelta(seconds=margin)}},
                                { "$unset": { "spider": "" } })
            except Exception as err:
                self.logger.error(err)
            time.sleep(3)

