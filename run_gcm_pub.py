#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
from redis import StrictRedis
from pprint import pprint
from gcm import GCM, GCMRequest, GCMError


if __name__ == "__main__":
    import anon

    from config import GOOGLE_API_KEY 
    from config import MONGODB_DATABASE, MONGODB_HOST, MONGODB_PORT
    from config import REDIS_PORT, REDIS_HOST
    from config import GCM_COLLAPSE_KEY
    
    mongoclient = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
    mongodb = mongoclient[MONGODB_DATABASE]
    r = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    sub = r.pubsub(ignore_subscribe_messages=True)
    sub.psubscribe("rigveda-article-push-*")

    print "Preaparing GCM to be able to push nofications"
    gcm = GCM(GOOGLE_API_KEY)

    print "Starting to serve GCM push service"
    try:
        while True:
            for msg in sub.listen():
                channel = msg["channel"]
                article = channel[len("rigveda-article-push-"):].decode("utf-8")
                print "[PUSHING]", article
                try:
                    rids = anon.query_gcm_registration_ids(mongodb, article)
                    if len(rids) == 0: 
                        continue
                    resp_dict = gcm.send(GCMRequest(rids, data={"article": article}, collapse_key=GCM_COLLAPSE_KEY))
                except GCMError as err:
                    print "[ERROR OCCURED]", err
                else:
                    for rid, t in resp_dict.items():
                        if t[0]:
                            if t[1] is not None:
                                canon_id = t[1]
                                anon.change_registraton_id(mongodb, rid, canon_id)
                        else: # FAIL
                            error = t[1]
                            if error == "InvalidRegistration" or error == "NotRegistered":
                                print "[IllegalRegistrationID]", rid
                                anon.remove_registration_id(mongodb, rid)
                            else:
                                print "[UNKNOWN GCM ERROR]", error

    finally:
        mongoclient.close()
