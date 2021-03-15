# -*- coding: utf-8 -*-
"""
@File: logger
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import datetime
from gli.logger import Logger
from gli.connect.mongodb import MongoDB
from datetime import datetime, timedelta

class ProxyIP(object):
    """Proxy IP
    """
    def __init__(self):
        self.logger = Logger(folder="zhima")
        client = MongoDB()
        if "local_rw" in client:
            self.reader = client["local_rw"]["proxies"]
        else:
            raise RuntimeError("The specified configuration item could not be found.")
        if "local_rw" in client:
            self.writer = client["local_rw"]["proxies"]
        else:
            raise RuntimeError("The specified configuration item could not be found.")

    def inc_used(self, id: object, field: str):
        """ Increase the number of used.
        Args:
            id: Proxy ID.
            field: Field name in proxy information.
        """
        try:
            self.writer["proxy"].update_one(
                { "_id": id },
                { "$set": { "update_time": datetime.now() }, "$inc": { "used": int(1), "spider.%s.used" % field: int(1) } })
        except Exception as err:
            self.logger.error(err)

    def inc_fail(self, id:object, field:str):
        """ Increase the number of fail.
        Args:
            id: Proxy ID.
            field: Field name in proxy information.
        """
        try:
            self.writer["proxy"].update_one(
                { "_id": id },
                { "$set": { "update_time": datetime.now() }, "$inc": { "fail": int(1), "spider.%s.fail" % field: int(1) } })
        except Exception as err:
            self.logger.error(err)

    def modify_update_time(self):
        """ Modify update time.
        Args:
            id: Proxy ID.
            field: Field name in proxy information.
        """
        try:
            self.reader["proxy"].find_one_and_update(
                {},
                { "$set": { "update_time": datetime.now() } },
                sort = [("update_time", -1)])
        except Exception as err:
            self.logger.error(err)

    def run(self, field:str, fail_weight: int = 20, margin: int = 10):
        """Get proxy IP address.
        Args:
            field: Field name in proxy information.
            fail_weight: Reduce selection probability after failure.
            margin: Expiration time margin of proxy IP.
        Returns:
            Success returns { "_id", "ip", "port" }, otherwise None.
        """
        try:
            cursor = self.reader["proxy"].aggregate([
                { "$match": { "expire_time": { "$gt": datetime.now() + timedelta(seconds=margin) } } },
                { "$addFields": { "count": { "$add": [ "$spider.%s.used" % field, { "$multiply": [ "$spider.%s.fail" % field, fail_weight ] } ] } } },
                { "$sort": { "count": 1 } },
                { "$project": { "ip": 1, "port": 1 } },
                { "$limit": 1 } ],
                allowDiskUse=False)
            for doc in cursor:
                # Increase the number of used.
                self.inc_used(doc["_id"], field)
                return doc
            # Modify update time.
            self.modify_update_time()
        except Exception as err:
            self.logger.error(err)
