#!/usr/bin/env python
# -*- coding: utf-8 -*-

import recentchanges

from flask import Flask, g, jsonify, request
from redis import StrictRedis
from pymongo import MongoClient
from crawl import get_epoch

from config import POLLING_MAX_LIMIT, MONGODB_HOST, MONGODB_PORT, REDIS_HOST, REDIS_PORT, MONGODB_DATABASE

app = Flask(__name__)

@app.before_request
def before_request():
    g.mongoclient = MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
    g.mongodb = g.mongoclient[MONGODB_DATABASE]
    g.redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)

@app.teardown_request
def teardown_request(exception):
    g.mongoclient.close()



@app.route("/", methods=["GET"])
def index():
    return "Hello?"

@app.route("/debug/top", methods=["GET"])
def debug_recent():
    imd = recentchanges.query_recentchanges(g.mongodb,
                                            g.redis,
                                            None,
                                            10,
                                            until=get_epoch(),
                                            desc=True,
                                            exclusive=(False, True))
    result = "<h1>Top 10</h1>"
    for x in imd:
        result += "<p>%s</p>"%x["article"]
    return result

@app.route("/rest/epoch")
def view_epoch():
    return str(get_epoch())


@app.route("/rest/recentchanges/poll_new", methods=["GET"])
def pdw():
    article = request.args.get("article", None)
    if article == "__all__":
        article = None
    if request.args["from"] == "now":
        _from = get_epoch()
    else:
        _from = float(request.args["from"])
    limit = min(int(request.args.get("limit", POLLING_MAX_LIMIT)), POLLING_MAX_LIMIT)

    imd = recentchanges.query_recentchanges(g.mongodb,
                                            g.redis,
                                            article,
                                            limit,
                                            _from=_from,
                                            until=None,
                                            desc=False,
                                            exclusive=(True, False))

    return jsonify(result=imd)


@app.route("/rest/recentchanges/poll_old", methods=["GET"])
def puw():
    article = request.args.get("article", None)
    if article == "__all__":
        article = None
    if request.args["until"] == "now":
        until = get_epoch()
    else:
        until = float(request.args["until"])
    limit = min(int(request.args.get("limit", POLLING_MAX_LIMIT)), POLLING_MAX_LIMIT)

    imd = recentchanges.query_recentchanges(g.mongodb,
                                            g.redis,
                                            article,
                                            limit,
                                            _from=None,
                                            until=until,
                                            desc=True,
                                            exclusive=(False, True))
    return jsonify(result=imd)


@app.route("/rest/keywords", methods=["GET"])
def kwds():
    prefix = request.args["prefix"]
    return jsonify(result=recentchanges.query_prefix(g.redis, 
                                                     prefix, 
                                                     limit=10))

if __name__ == "__main__":
    app.run(debug=True)
