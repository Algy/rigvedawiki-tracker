#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
from pymongo import ASCENDING, DESCENDING 
from pymongo.errors import DuplicateKeyError

class SubscribeError(Exception):
    pass


def _create_uuid():
    from uuid import uuid4
    return str(uuid4())


def make_anon_account(mongodb):
    anon_id = _create_uuid()
    created_at = datetime.utcnow()
    mongodb.anon_user.insert({"anon_id": anon_id, 
                              "created_at": created_at})
    return anon_id

def has_anon(mongodb, anon_id):
    z = mongodb.anon_user.find({"anon_id": anon_id}).limit(1).count(True)
    if z:
        return True
    else:
        return False

def anon_sub_article(mongodb, anon_id, article):
    d = mongodb.anon_user.find_one({"anon_id": anon_id})
    if d is None:
        raise SubscribeError("no_such_anon_id", "{0} is invalid id".format(anon_id), 404)
    try:
        mongodb.assoc_anon_sub.insert({"anon_id": anon_id,
                                       "article": article})
    except DuplicateKeyError:
        raise SubscribeError(
            "already_subscribed", 
            "article %s is already subscribed"%article, 404)


def anon_unsub_article(mongodb, anon_id, article):
    d = mongodb.anon_user.find_one({"anon_id": anon_id})
    if d is None:
        raise SubscribeError("no_such_anon_id", "{0} is invalid id".format(anon_id), 404)
    res = mongodb.assoc_anon_sub.remove({"anon_id": anon_id,
                                         "article": article})
    if res is None:
        raise SubscribeError("no_such_article", 
                             "{0} is not subscribed for {1}".format(article, anon_id), 404)


def change_registraton_id(mongodb, old_r_id, new_r_id):
    res = mongodb.gcm_push.update({"registration_id": old_r_id},
                                  {"$set": {"registration_id": new_r_id,
                                            "registered_at": datetime.utcnow()}})
    return res is not None


def list_subs(mongodb, anon_id): 
    _iter = mongodb.assoc_anon_sub.find({"anon_id": anon_id}, {"article": True})
    return [x["article"] for x in _iter]

def get_registraion_id(mongodb, anon_id):
    d = mongodb.gcm_push.find_one({"anon_id": anon_id})
    if d is None:
        return None # not found
    return d["registration_id"] 


def remove_registration_id(mongodb, registration_id):
    r = mongodb.gcm_push.remove({"registration_id": registration_id})
    return r is not None
    

def set_gcm(mongodb, anon_id, registration_id):
    if not has_anon(mongodb, anon_id):
        raise SubscribeError("no_such_anon_id", "", 404)
    res = mongodb.gcm_push.update(
        {"anon_id": anon_id},
        {"anon_id": anon_id, 
         "registration_id": registration_id,
         "registered_at": datetime.utcnow()},
        upsert=True)
    return res is not None


def query_gcm_registration_ids(mongodb, article):
    _iter = mongodb.assoc_anon_sub.find({"article": article},
                                        {"anon_id": True})
    res = mongodb.gcm_push.find(
        {"anon_id": {"$in": [x["anon_id"] for x in _iter]}},
        {"registration_id": True})
    res = list(res)
    return [x["registration_id"] for x in res]



def ensure_indices(mongodb):
    mongodb.anon_user.ensure_index([("anon_id", ASCENDING)], unique=True)
    mongodb.assoc_anon_sub.ensure_index([("anon_id", ASCENDING), 
                                         ("article", ASCENDING)],
                                        unique=True,
                                        dropDups=True)
    mongodb.gcm_push.ensure_index([("registration_id", ASCENDING),
                                   ("anon_id", ASCENDING)],
                                  unique=True,
                                  dropDups=True)
