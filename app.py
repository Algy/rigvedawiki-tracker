#!/usr/bin/env python
# -*- coding: utf-8 -*-

import recentchanges
import anon

from flask import Flask, g, jsonify, request, Response
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



@app.route("/", methods=["GET", "POST"])
def index():
    return Response(str(request.headers) + "<br />" + str(request.json))

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
                                            limit + 1,
                                            _from=_from,
                                            until=None,
                                            desc=False,
                                            exclusive=(True, False))
    flooded = len(imd) > limit
    if flooded:
        imd.pop()
    return jsonify(result=imd, flooded=flooded)


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
                                            limit + 1,
                                            _from=None,
                                            until=until,
                                            desc=True,
                                            exclusive=(False, True))
    flooded = len(imd) > limit
    if flooded:
        imd.pop()
    return jsonify(result=imd, flooded=flooded)

def exc_to_error(exc):
    try:
        code = exc.args[2]
    except IndexError:
        code = 404

    return make_error(exc.args[0], exc.args[1], code)


def make_error(error, msg, err_code=404):
    result = jsonify(error=error, msg=msg)
    result.status_code = err_code
    return result


@app.route("/rest/anon/new", methods=["GET"])
def new_anon():
    anon_id = anon.make_anon_account(g.mongodb)
    return jsonify(anon_id=anon_id)


@app.route("/rest/anon/subscribe", methods=["POST"])
def anon_subscribe():
    '''
    Output -
        200 OK
        {
            ok: true
        }

        404 Not Found
        {
            error: "no_such_anon_id" | "already_subscribed"
            msg: ...
        }

    '''
    article = request.form["article"]
    anon_id = request.form["anon_id"]

    try:
        anon.anon_sub_article(g.mongodb, anon_id, article)
    except anon.SubscribeError as err:
        return exc_to_error(err)
    return jsonify(ok=True)

@app.route("/rest/anon/unsubscribe", methods=["POST"])
def anon_unsubscribe():
    '''
    Output -
        200 OK
        {
            ok: true
        }

        404 Not Found
        {
            error: "no_such_anon_id" | "no_such_article"
            msg: ...
        }
    '''
    article = request.form["article"]
    anon_id = request.form["anon_id"]

    try:
        anon.anon_unsub_article(g.mongodb, anon_id, article)
    except anon.SubscribeError as err:
        return exc_to_error(err)
    return jsonify(ok=True)

@app.route("/rest/anon/sublist", methods=["GET"])
def anon_list_subs():
    anon_id = request.args["anon_id"]
    return jsonify(result=anon.list_subs(g.mongodb, anon_id))


@app.route("/rest/anon/registration_id", methods=["GET", "POST"])
def reg_id():
    if request.method == "GET":
        anon_id = request.args["anon_id"]
        rid = anon.get_registraion_id(g.mongodb, anon_id)
        return jsonify(result=rid)
    elif request.method == "POST":
        anon_id = request.form["anon_id"]
        registration_id = request.form["registration_id"]
        try:
            if anon.set_gcm(g.mongodb, anon_id, registration_id):
                return jsonify(ok=True)
            else:
                return make_error("failed", "Failed to register the id. Try again.")
        except anon.SubscribeError as err:
            return exc_to_error(err)

@app.route("/debug/rid", methods=["GET"])
def _adssdfa():
    article = request.args["article"]
    return jsonify(result=anon.query_gcm_registration_ids(g.mongodb, article))



@app.route("/rest/keywords", methods=["GET"])
def kwds():
    prefix = request.args["prefix"]
    limit = int(request.args.get("limit", 10))
    candidiates = recentchanges.query_prefix(g.redis, prefix, limit=limit + 1)

    if len(candidiates) > limit:
        flooded = True
        candidiates.pop()
    else:
        flooded = False
    
    return jsonify(result=candidiates, flooded=flooded)

@app.route("/echo", methods=["GET"])
def safd():
    return repr(dict(request.args))

if __name__ == "__main__":
    with MongoClient(host=MONGODB_HOST, port=MONGODB_PORT) as client:
        db = client[MONGODB_DATABASE]
        recentchanges.ensure_indices(db)
        anon.ensure_indices(db)
    app.run(debug=True)
