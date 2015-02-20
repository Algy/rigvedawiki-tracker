#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gcm import GCM, GCMRequest, GCMError

API_KEY = "AIzaSyCksKXPienmrnpoH7iDzUnDMjJy9CtS7eI"
REGISTRAION_ID = "APA91bHT6pRG6D5gw08hd-PCYB6MOs_uym00mPNoT0zKH1JB52tcg4HvBQrKSU5rWyhV7NxDhp3f2Fn37hmMMKslpXMFO9QvOjkENBV1qFn5sf_pqjaDlUfd_Y2_PJJwZ-C1L3I4ckzimDKlG8HzXUckYqI2jaCUr9_WQ3qkm8v2KO3rne_EBTg"

REQ_KWDS = {
    "data": {
        "1": 2
    }
}


if __name__ == "__main__":
    gcm = GCM(API_KEY)
    try:
        resp_dict = gcm.send(GCMRequest([REGISTRAION_ID], **REQ_KWDS))
    except GCMError as err:
        print "[ERROR OCCURED]", err
    else:
        (suc, rem) = resp_dict[REGISTRAION_ID]
        if suc:
            print "Success(canon_id:", rem, ")"
        else:
            print "Failed...", rem
