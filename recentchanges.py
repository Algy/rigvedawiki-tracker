#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from pymongo import ASCENDING, DESCENDING
from redis import WatchError
from struct import pack
from crawl import RigvedaLogItem
from config import RIGVEDA_CACHE_SIZE 

def _redis_range_strs(_from, until, exclusive=(False, False)):
    start = "(" if exclusive[0] else ""
    end = "(" if exclusive[1] else ""

    if _from is None:
        start += "-inf"
    else:
        start += str(_from)
    
    if until is None:
        end += "+inf"
    else:
        end += str(until)
    return start, end

def _all_prefix(s):
    if len(s) > 0:
        acc = ""
        idx = 0
        while idx < len(s):
            acc += s[idx]
            yield acc
            idx += 1

def _inc_str(s):
    result = ""
    carry_out = 1
    for idx in xrange(len(s) - 1, -1, -1):
        cur = carry_out + ord(s[idx])
        carry_out = cur / 256
        cur = cur % 256
        result = chr(cur) + result
    return result

def dictify_rigveda_log(log):
    return {
        "article": log.article,
        "gathered_at": log.gathered_at,
        "cmd": log.cmd,
        "author": log.author,
        "time_magic": log.time_magic,
        "edit_num": log.edit_num,
        "diff": json.dumps(log.diff),
        "comment": log.comment
    }

def objectify_rigveda_log(d):
    result = RigvedaLogItem(d["cmd"], d["article"], d["author"], 
                            d["time_magic"], d["edit_num"], d["comment"],
                            json.loads(d["diff"]))
    result.gathered_at = d["gathered_at"]
    return result

def pack_log_dict(d):
    return d["article"].encode("utf-8") + "\n" + json.dumps(d)

def log_is_about(packed_str, article):
    if article is None:
        return True
    else:
        return packed_str.startswith(article.encode("utf-8") + "\n")

def unpack_log_str(packed_str):
    bar_idx = packed_str.find("\n")
    return json.loads(packed_str[bar_idx+1:])

def filter_cache_chunk(cache_chunk, article, limit):
    result = []
    count = 0
    for x in cache_chunk:
        if not log_is_about(x, article):
            continue
        if count >= limit:
            break
        result.append(unpack_log_str(x))
        count += 1
    return result


def add_recentchanges(mongodb, redis, fresh_list):
    list_len = len(fresh_list)
    if list_len == 0:
        return
    dictified_list = map(dictify_rigveda_log, fresh_list)
    def added_args(limit=None):
        for idx, d in enumerate(dictified_list):
            if limit is not None and idx >= limit:
                break
            yield d["gathered_at"]
            yield pack_log_dict(d)

    mongodb.recentchange.insert(dictified_list) # cause side-effect to dictified_list.
                                                # That is, insert "_id" field into all of dict elements of it.
    for d in dictified_list:
        d.pop("_id")

    with redis.pipeline() as pipe:
        # cache update
        while True:
            try:
                pipe.watch("rigveda-recentchange-cache")
                cache_count = pipe.zcard("rigveda-recentchange-cache")

                if cache_count + list_len > RIGVEDA_CACHE_SIZE:
                    rem = cache_count + list_len - RIGVEDA_CACHE_SIZE
                    # evicting cache slots
                    dest = pipe.zrangebyscore("rigveda-recentchange-cache",
                                              min="-inf",
                                              max="+inf",
                                              withscores=True,
                                              start=rem-1,
                                              num=1)
                    if len(dest) > 0:
                        score = dest[0][1]
                        pipe.zremrangebyscore("rigveda-recentchange-cache",
                                              min="-inf",
                                              max=score)
                        cached_elem_num = list_len
                    else:
                        # TOO MANY ELEMENTS FROM FRESH LIST
                        pipe.zremrangebyscore("rigveda-recentchange-cache", 
                                               min="-inf", 
                                               max="+inf")
                        cached_elem_num = RIGVEDA_CACHE_SIZE
                else:
                    cached_elem_num = list_len
                pipe.zadd("rigveda-recentchange-cache", *list(added_args(cached_elem_num)))
                pipe.execute()
                break
            except WatchError:
                continue


def _recentchanges_cache_miss(mongodb, article, limit, 
                              _from=None, until=None, desc=True,
                              exclusive=(False, False)):
    if _from is None and until is None:
        cond = None
    else:
        cond = {}
        if _from is not None:
            if exclusive[0]:
                cond["$gt"] = _from
            else:
                cond["$gte"] = _from
        if until is not None:
            if exclusive[1]:
                cond["$lt"] = until
            else:
                cond["$lte"] = until
    spec = {}
    if article is not None:
        spec["article"] = article
    if cond is not None:
        spec["gathered_at"] = cond
    sort = [("gathered_at", (DESCENDING if desc else ASCENDING))]
    return list(mongodb.recentchange.find(spec=spec, 
                                          fields={"_id": False},
                                          limit=limit, 
                                          sort=sort))

def _cache_least_score(pipe, key):
    t = pipe.zrangebyscore(key,
                           min="-inf",
                           max="+inf",
                           start=0,
                           num=1,
                           withscores=True)
    if len(t) == 0:
        # No slots are in the cache. Thus, Cache miss occurs
        return None
    return t[0][1]

def _asc_query_rc(mongodb, redis, article, limit, 
                  _from=None, until=None, exclusive=(False, False)):
    # Determine if the whole range of time 
    # in which we demand information could be covered by the cache.
    with redis.pipeline() as pipe:
        while True:
            try:
                pipe.watch("rigveda-recentchange-cache")
                least_recent_slot_time = _cache_least_score(
                    pipe,
                    "rigveda-recentchange-cache")
                if _from is not None and _from >= least_recent_slot_time:
                    start, end = _redis_range_strs(_from, until, exclusive)
                    cache = pipe.zrangebyscore("rigveda-recentchange-cache", min=start, max=end)
                    return filter_cache_chunk(cache, article, limit)
                else:
                    break
            except WatchError:
                continue
    return _recentchanges_cache_miss(mongodb, article, limit, _from, until, False, exclusive)

def _desc_query_rc(mongodb, redis, article, limit,
                   _from=None, until=None, exclusive=(False, False)):
    no_use_cache = False
    desc_cache = None

    with redis.pipeline() as pipe:
        while True:
            try:
                pipe.watch("rigveda-recentchange-cache")
                least_recent_slot_time = _cache_least_score(
                    pipe,
                    "rigveda-recentchange-cache")
                if least_recent_slot_time is None or least_recent_slot_time > until:
                    # the cache is empty or doesn't include any part of the range
                    no_use_cache = True
                    break
        
                start, end = _redis_range_strs(_from, until, exclusive)
                desc_cache = pipe.zrevrangebyscore("rigveda-recentchange-cache", min=start, max=end)
                pipe.execute()
                break
            except WatchError:
                continue
    if no_use_cache:
        return _recentchanges_cache_miss(mongodb, article, 
                                         limit, _from, until, 
                                         True,
                                         exclusive)
    desc_cache = filter_cache_chunk(desc_cache, article, limit)
    desc_cache_len = len(desc_cache)

    if desc_cache_len < limit and (_from is None or _from < least_recent_slot_time):
        # cache miss
        noncache = _recentchanges_cache_miss(mongodb, 
                                             article,
                                             limit - desc_cache_len,
                                             _from,
                                             min(least_recent_slot_time, until),
                                             True,
                                             (exclusive[0], True))
        return desc_cache + noncache
    else:
        return desc_cache[:limit]


def query_recentchanges(mongodb, redis, article, limit, 
                        _from=None, until=None, desc=True, exclusive=(False, False)):
    '''
    '''
    if desc:
        return _desc_query_rc(mongodb, redis, article, limit, _from, until, exclusive)
    else:
        return _asc_query_rc(mongodb, redis, article, limit, _from, until, exclusive)
    




def add_keyword(redis, keyword):
    if isinstance(keyword, unicode):
        keyword = keyword.encode("utf-8")

    if not redis.sismember("rigveda-keywords", keyword):
        with redis.pipeline() as p:
            p.sadd("rigveda-keywords", keyword)
            for prefix in _all_prefix(keyword):
                p.zadd("rigveda-keyword-prefix", 0, prefix)
            p.execute()
        return True
    else:
        return False

def delete_keyword(redis, keyword):
    if isinstance(keyword, unicode):
        keyword = keyword.encode("utf-8")

    if redis.sismember("rigveda-keywords", keyword):
        with redis.pipeline() as p:
            p.srem("rigveda-keywords", keyword)
            for prefix in _all_prefix(keyword):
                p.zrem("rigveda-keyword-prefix", prefix)
            p.execute()
        return True
    else:
        return False


def query_prefix(redis, prefix, limit):
    assert len(prefix) > 0

    if isinstance(prefix, unicode):
        prefix = prefix.encode("utf-8")
    inced_prefix = _inc_str(prefix)
    return redis.zrangebylex("rigveda-keyword-prefix", "[" + prefix, "(" + inced_prefix, num=limit)


def ensure_indices(mongodb):
    mongodb.recentchanges.ensure_index([("gathered_at", DESCENDING), ("article", ASCENDING)])

if __name__ == "__main__":
    pass
