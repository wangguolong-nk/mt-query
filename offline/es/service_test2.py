# -*- coding:utf-8 -*-
import urllib
import urllib2
import sys

if __name__=='__main__':


    mode=sys.argv[1]
    updateUrl = "http://192.168.134.154:9090/meipai-query-suggest-api/updateOrInsert"
    queryUrl = "http://192.168.134.154:9090/meipai-query-suggest-api/query"
    deleteUrl = "http://192.168.134.154:9090/meipai-query-suggest-api/deleteById"
    if mode=='update':
        query=sys.argv[2]
        id=sys.argv[3]
        post_data = urllib.urlencode({'query': query, 'id': id, 'weight': 1.2})
        print updateUrl,post_data
        req = urllib2.Request(updateUrl,post_data)
        req_res = urllib2.urlopen(req, post_data)
        res = req_res.read()
        print 'res=', res
    elif mode=='query':
        query = sys.argv[2]
        id = sys.argv[3]
        post_data = urllib.urlencode({'query': query, 'id': id, 'from': 0,'size':20})
        print queryUrl,post_data
        req = urllib2.Request(queryUrl, post_data)
        req_res = urllib2.urlopen(req, post_data)
        res = req_res.read()
        print 'res=', res
    elif mode=='delete':
        id = sys.argv[2]
        post_data = urllib.urlencode({'id': id})
        print deleteUrl,post_data
        req = urllib2.Request(deleteUrl, post_data)
        req_res = urllib2.urlopen(req, post_data)
        res = req_res.read()
        print 'res=', res