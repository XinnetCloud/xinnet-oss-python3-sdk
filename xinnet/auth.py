# -*- coding: utf-8 -*-

import hmac
#import sha
import hashlib
import urllib
import base64
from .tools import to_bytes 

def make_signature(secret,date, bucket, object):
    string_to_sign = '{}\n\n\n{}\n/{}/{}'.format('GET',date,bucket,object) 
    h = hmac.new(to_bytes(secret), to_bytes(string_to_sign), hashlib.sha1)
    return urllib.parse.quote(base64.encodestring(h.digest()).strip())

class Auth(object):
    """用于保存用户AccessKeyId、AccessKeySecret，以及计算签名的对象。"""
    def __init__(self, access_key_id, access_key_secret):
        self.id = access_key_id.strip()
        self.secret = access_key_secret.strip()
