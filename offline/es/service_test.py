# -*- coding:utf-8 -*-
import urllib
import urllib2
import sys

if __name__=='__main__':


    if len(sys.argv)>1:
        query=sys.argv[1]
        id=sys.argv[2]
    # url = 'http://192.168.134.154:9090/meipai-query-suggest-api/updateOrInsert?query='+query+'&id='+id+'&weight=1.2'
    url = 'http://192.168.134.154:9090/meipai-query-suggest-api/updateOrInsert'
    print url
    req = urllib2.Request(url)
    post_data = urllib.urlencode({'query':query,'id':id,'weight':1.2})
    print post_data
    req_res = urllib2.urlopen(req,post_data)
    res = req_res.read()
    print 'res=', res
    # url = 'http://192.168.134.154:9090/meipai-query-suggest-api/query?query=测试123&uid=10&from=0&size=20'
    url = 'http://192.168.134.154:9090/meipai-query-suggest-api/query'
    print url
    req = urllib2.Request(url)
    post_data = urllib.urlencode({'query': query, 'id': id, 'from': 0, 'size': 20})
    print post_data
    req_res = urllib2.urlopen(req,post_data)
    res = req_res.read()
    print 'res=', res
    url='http://192.168.134.154:9090/meipai-query-suggest-api/deleteById'
    print url
    req = urllib2.Request(url)
    post_data = urllib.urlencode({'id': id})
    print post_data
    req_res = urllib2.urlopen(req,post_data)
    res = req_res.read()
    print 'res=', res
    url = 'http://192.168.134.154:9090/meipai-query-suggest-api/query'
    print url
    req = urllib2.Request(url)
    post_data = urllib.urlencode({'query': query, 'id': id, 'from': 0, 'size': 20})
    print post_data
    req_res = urllib2.urlopen(req,post_data)
    res = req_res.read()
    print 'res=', res


'''


http://192.168.134.154:9090/meipai-query-suggest-api/updateOrInsert?query=测试123456789&id=22&weight=1.3

http://192.168.134.154:9090/meipai-query-suggest-api/query?query=测试123456789&uid=22&from=0&size=20

http://192.168.134.154:9090/meipai-query-suggest-api/deleteById?id=22

'''