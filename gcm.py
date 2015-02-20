import json

from urllib2 import Request, urlopen, URLError, HTTPError

GOOGLE_GCM_URL = "https://android.googleapis.com/gcm/send"

class GCMError(Exception):
    pass

class GCMConnectionError(GCMError):
    pass

class GCMInvalidAPIKey(GCMError):
    pass

class GCMInternalError(GCMError):
    pass


class GCMRequest:
    def __init__(self, registration_ids,
                 data=None,
                 delay_while_idle=False,
                 time_to_live=None,
                 restricted_package_name=None,
                 collapse_key=None, 
                 dry_run=False):
        assert isinstance(registration_ids, (list, tuple,))
        if data is not None:
            assert isinstance(data, dict)
        self.registration_ids = registration_ids
        self.delay_while_idle = delay_while_idle
        self.time_to_live = time_to_live
        self.restricted_package_name = restricted_package_name
        self.collapse_key = collapse_key
        self.data = data
        self.dry_run = dry_run

    def dictify(self):
        res = {"registration_ids": self.registration_ids,
               "delay_while_idle": self.delay_while_idle,
               "dry_run": self.dry_run}
        if self.time_to_live is not None:
            res["time_to_live"] = self.time_to_live
        if self.restricted_package_name is not None:
            res["restricted_package_name"] = self.restricted_package_name
        if self.collapse_key is not None:
            res["collapse_key"] = self.collapse_key
        if self.data is not None:
            res["data"] = self.data
        return res

    def jsonify(self):
        result = json.dumps(self.dictify())
        print result
        return result


class GCM(object):
    def __init__(self, api_key):
        self.api_key = api_key

    def send(self, request, retry_after=None):
        url_req = Request(url=GOOGLE_GCM_URL,
                          headers={"Content-Type": "application/json",
                                   "Authorization": "key=%s"%self.api_key},
                          data=request.jsonify())
        try:
            url_resp = urlopen(url_req)
        except URLError as err:
            raise GCMConnectionError(str(err))
            
        if url_resp.code == 401:
            raise GCMInvalidAPIKey("Unauthorized API Key")
        elif 500 <= url_resp.code <= 599:
            raise GCMInternalError("Error Code %d"%url_resp.code)
        res = json.load(url_resp)

        
        ret = {}
        for idx, d in enumerate(res["results"]):
            if "error" in d:
                ret[request.registration_ids[idx]] = (False, d["error"])
            else:
                canon_id = d.get("registration_id", None)
                ret[request.registration_ids[idx]] = (True, canon_id)
        return ret
