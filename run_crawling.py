#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
import json

from pymongo import MongoClient
from redis import StrictRedis
from recentchanges import add_recentchanges, add_keyword, delete_keyword, dictify_rigveda_log
from time import sleep, time
from crawl import crawl_once, get_epoch
from config import CRAWLING_PERIOD, MONGODB_HOST, MONGODB_PORT, REDIS_HOST, REDIS_PORT, MONGODB_DATABASE

def make_store_fun(mongodb, redis):
    def store_fun(log_list):
        add_recentchanges(mongodb, redis, log_list)
        for log in log_list:
            if log.cmd == "delete":
                delete_keyword(redis, log.article)
            else:
                add_keyword(redis, log.article)
    return store_fun


def format_channel_name(article):
    return "rigveda-article-push-{0}".format(article.encode("utf-8"))

def make_publish_fun(mongodb, redis, pub_redis):
    def publish_fun(log_list):
        if log_list:
            pub_logs = sorted(
                log_list, 
                lambda lhs, rhs: (
                    lhs.article < rhs.article or 
                    (lhs.article == rhs.article and
                     lhs.gathered_at < rhs.gathered_at)))
            
            idx = 0
            while idx < len(pub_logs):
                article = pub_logs[idx].article
                lst = []
                while idx < len(pub_logs) and article == pub_logs[idx].article:
                    lst.append(pub_logs[idx])
                    idx += 1
                
                pub_redis.publish(format_channel_name(article),
                                  json.dumps(map(dictify_rigveda_log, lst)))

            print "[COLLECTED ITEMS]"
            for l in log_list:
                print " ", l
    return publish_fun
    


def serve_forever(mongodb, redis, pub_redis, crawl_once=crawl_once):
    old_list = crawl_once(None)
    old_time = time()

    store_fun = make_store_fun(mongodb, redis)
    publish_fun = make_publish_fun(mongodb, redis, pub_redis)
    while True:
        old_list = crawl_once(old_list, store_fun, publish_fun)
        '''
        except Exception as err:
            print "[ERROR]", err
        '''

        new_time = time()
        while new_time <= old_time + CRAWLING_PERIOD:
            sleep(0.1)
            new_time = time()
        old_time = new_time


def fake_crawl_once(old, store_fun=None, publish_fun=None):
    import random
    import time
    from crawl import RigvedaLogItem
    if old is None:
        return []

    idx = 0
    logs = []
    while True:
        diff = "No Diff"
        time_magic = "00:00 [12:32]"
        article = raw_input("ARTICLE(%d)>>>"%idx).decode("utf-8")
        author = raw_input("AUTHOR(%d)>>>"%idx).decode("utf-8")
        if not article:
            break
        log_item = RigvedaLogItem("modify", article, author, time_magic, diff=diff)
        log_item.gathered_at = get_epoch()
        logs.append(log_item)
        idx += 1
    if store_fun:
        store_fun(logs)
    if publish_fun:
        publish_fun(logs)
    return [] 


if __name__ == "__main__":
    print "Starting crawling..."
    mongoclient = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
    mongodb = mongoclient[MONGODB_DATABASE]
    redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    pubsub_redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    pubsub = pubsub_redis.pubsub()

    try:
        serve_forever(mongodb, redis, pubsub_redis)
    finally:
        mongoclient.close()
