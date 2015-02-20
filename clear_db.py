#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
from redis import StrictRedis
from config import MONGODB_HOST, MONGODB_PORT, REDIS_HOST, REDIS_PORT, MONGODB_DATABASE


def purge():
    mongoclient = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
    redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)

    try:
        mongoclient.drop_database(MONGODB_DATABASE)
        redis.delete("rigveda-keywords")
        redis.delete("rigveda-recentchange-cache")
    finally:
        mongoclient.close()


if __name__ == "__main__":
    resp = raw_input("Are you sure to clear db and redis cache?")
    if resp.startswith("y") or resp.startswith("Y"):
        resp = raw_input("Really..?")
        if resp.startswith("y") or resp.startswith("Y"):
            print "OK. I'll do"
            purge()
        else:
            print "Canceled"
    else:
        print "Canceled"
