#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import hmac 
import requests
import datetime
import hashlib


import xml.etree.ElementTree as ElementTree

def to_bytes(data):
    if isinstance(data, str):
        return data.encode(encoding='utf-8')
    else:
        return data

def generate_sign(secret,sign):
    h = hmac.new(secret,sign,hashlib.sha1)
    signature = base64.b64encode(to_bytes(h.digest()))
    return signature.decode('utf-8')

def get_gmttime():
    gmt_format = '%a, %d %b %Y %H:%M:%S GMT'
    gmt_time = datetime.datetime.utcnow().strftime(gmt_format)
    return gmt_time
