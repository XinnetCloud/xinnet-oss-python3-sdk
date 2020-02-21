# -*- coding: utf-8 -*-

from .tools import *
from .auth import *
import re
import time

BUCKET_ACL_PRIVATE = 'private'
BUCKET_ACL_PUBLIC_READ = 'public-read'
BUCKET_ACL_PUBLIC_READ_WRITE = 'public-read-write'

class SimplifiedObjectInfo(object):
    def __init__(self, key, last_modified, etag, size, storage_class):
        #: 文件名。
        self.key = key
        #: 文件的最后修改时间
        self.last_modified = last_modified
        #: HTTP ETag
        self.etag = etag
        #: 文件大小
        self.size = size
        #: 文件的存储类别，是一个字符串。
        self.storage_class = storage_class

class SimpleBucketInfo(object):
    def __init__(self, name, creation_date):
        #: bucket名。
        self.name = name
        #: bucket创建时间
        self.creation_date = creation_date

        

class Bucket(object): 
    def __init__(self, auth, endpoint, bucket_name=''):
        self.bucket = bucket_name.strip()
        self.endpoint = endpoint[:len(endpoint)-1] if (endpoint[-1] == '/' ) else endpoint  
        self.auth = auth
        self.prefix = '/v1/storage/oss/'

    def create_bucket(self,bucket_acl = BUCKET_ACL_PRIVATE):
        acl = 'x-amz-acl:{}'.format(bucket_acl)
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n{}\n/{}/'.format('PUT',gmt_time,acl,self.bucket)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'x-oss-acl':bucket_acl,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket
        r = requests.put(url,headers=headers)
        return r

    def list_all_bucket(self):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/'.format('GET',gmt_time)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = '{}{}'.format(self.endpoint,self.prefix[:len(self.prefix)-1])
        r = requests.get(url,headers=headers)
        if r.status_code != 200:
            return r.content.decode('utf-8')
        name = re.findall("<Name>(.+?)</Name>",r.content.decode('utf-8'))
        create_date = re.findall("<CreationDate>(.+?)</CreationDate>",r.content.decode('utf-8'))
        info = []
        for i in range (0,len(name)):
            l = SimpleBucketInfo(name[i],create_date[i])
            info.append(l)
        return info
       
    def get_bucket_info(self):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/'.format('GET',gmt_time,self.bucket)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket +'/' 
        r = requests.get(url,headers=headers)
        if r.status_code != 200:
            return r.content.decode('utf-8')

        key = re.findall("<Key>(.+?)</Key>",r.content.decode('utf-8'))
        LastModified = re.findall("<LastModified>(.+?)</LastModified>",r.content.decode('utf-8'))
        eTag = re.findall("<ETag>(.+?)</ETag>",r.content.decode('utf-8'))
        size = re.findall("<Size>(.+?)</Size>",r.content.decode('utf-8'))
        storageClass = re.findall("<StorageClass>(.+?)</StorageClass>",r.content.decode('utf-8'))
        objResult = []
        for i in range (0,len(key)):
            objinfo = SimplifiedObjectInfo(key[i],LastModified[i],eTag[i],size[i],storageClass[i])
            objResult.append(objinfo)

        return objResult
   
    def delete_bucket(self):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/'.format('DELETE',gmt_time,self.bucket)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)                                     
        headers = {'Authorization':auth,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket                                       
        r = requests.delete(url,headers=headers)                                              
        return r 


    def put_bucket_acl(self,acl=BUCKET_ACL_PRIVATE):

        acl_str='x-amz-acl:{}'.format(acl)
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n{}\n/{}/?acl'.format('PUT',gmt_time,acl_str,self.bucket)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'x-oss-acl':acl,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket +'/'
        r = requests.put(url,headers = headers,params = 'acl')
        return bytes.decode(r.content)  
    
    def get_bucket_acl(self):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/?acl'.format('GET',gmt_time,self.bucket)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket +'/'
        r = requests.get(url,headers = headers,params = 'acl')
        if r.status_code != 200:
            return bytes.decode(r.content)
        result = re.findall("<Permission>(.+?)</Permission>",r.content.decode('utf-8'))
        if len(result) == 1: 
            ret = BUCKET_ACL_PRIVATE
        elif len(result) == 2:
            ret = BUCKET_ACL_PUBLIC_READ
        else :
            ret = BUCKET_ACL_PUBLIC_READ_WRITE
        return ret

    def put_object(self,object,data,acl = BUCKET_ACL_PRIVATE):

        acl_str='x-amz-acl:{}'.format(acl)
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n{}\n/{}/{}'.format('PUT',gmt_time,acl_str,self.bucket,object)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'x-oss-acl':acl,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket + '/' + object
        r = requests.put(url,headers=headers,data=data)
        return r   

    def put_object_from_file(self,object,filename,acl = BUCKET_ACL_PRIVATE):
         with open(filename, 'rb') as f:
             return self.put_object(object, f.read(),acl)

    def sign_url(self,object,expires=60):
        expiration_time = str(int(time.time()) + expires)
        signature = make_signature(self.auth.secret,expiration_time, self.bucket, object)
        sign_str = '{}={}&{}={}&{}={}'.format('AWSAccessKeyId',self.auth.id,\
                                             'Expires',expiration_time,\
                                             'Signature',signature)
        url = self.endpoint +'/' + self.bucket + '/'+ object + '?' + sign_str
        return url

    def delete_object(self,objectname):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/{}'.format('DELETE',gmt_time,self.bucket,objectname)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket + '/' + objectname
        r = requests.delete(url,headers=headers)
        return r
    
    def batch_delete_objects(self,list_object):
        fail_list = []
        nu = 0
        for i in list_object:
            ret = self.delete_object(i)
            if ret.status_code != 204:
                nu += 1
                fail_list.append(i)
        ret = {"fail":nu,"fail_list":fail_list}
        return ret


    def put_object_acl(self,object,acl):
        acl_str='x-amz-acl:{}'.format(acl)
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n{}\n/{}/{}?acl'.format('PUT',gmt_time,acl_str,self.bucket,object)
        #signature = generate_sign(self.auth.secret,sign)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'x-oss-acl':acl,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket +'/' + object 
        r = requests.put(url,headers = headers,params = 'acl')
        return bytes.decode(r.content)  


    def get_object_acl(self,object):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/{}?acl'.format('GET',gmt_time,self.bucket,object)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = self.endpoint + self.prefix + self.bucket +'/' + object
        r = requests.get(url,headers = headers,params = 'acl')
        if r.status_code != 200:
            return bytes.decode(r.content)

        result = re.findall("<Permission>(.+?)</Permission>",r.content.decode('utf-8'))
        if len(result) == 1: 
            ret = BUCKET_ACL_PRIVATE
        elif len(result) == 2:
            ret = BUCKET_ACL_PUBLIC_READ
        else:
            ret = BUCKET_ACL_PUBLIC_READ_WRITE
        return ret

    def get_object(self,object):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/{}'.format('GET',gmt_time,self.bucket,object)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = '{}{}{}/{}'.format(self.endpoint,self.prefix,self.bucket,object)
        r = requests.get(url,headers=headers)
        return r
       
    def get_object_to_file(self,object,filepath):
        with open(filepath, 'wb') as f:
            result = self.get_object(object)
            if result.status_code != 200:
                return result.status_code
            f.write(result.content)

    def head_object(self,object):
        gmt_time = get_gmttime()
        sign = '{}\n\n\n{}\n/{}/{}'.format('HEAD',gmt_time,self.bucket,object)
        signature = generate_sign(to_bytes(self.auth.secret),to_bytes(sign))
        auth = 'AWS {}:{}'.format(self.auth.id,signature)
        headers = {'Authorization':auth,'Date':gmt_time}
        url = '{}{}{}/{}'.format(self.endpoint,self.prefix,self.bucket,object)
        r = requests.head(url,headers=headers)
        return r


    def does_bucket_exist(self):
        m = self.get_bucket_info()
        if isinstance(m,str):
            if 'NoSuchBucket' in m:
                return False
            else:
                return m
        else:
            return True

    def object_exists(self,object):
        m = self.head_object(object)
        if m.status_code not in range(200,300):
            return False
        return True
