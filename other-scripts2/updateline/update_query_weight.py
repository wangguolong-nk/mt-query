# -*- coding:utf-8 -*-
import md5
import urllib2
import sys
from urllib import quote
import json

# src = 'this is a md5 test.'
# m1 = md5.new()
# m1.update(src)
# print m1.hexdigest()
# m1.update(src)
# print m1.hexdigest(),type(m1.hexdigest())
# sys.exit(2)


deploydir='/data1/wgl/deploy_data'
query_quality_dir=deploydir+'/work_data/search'
lowpath=query_quality_dir+'/query.predict.train.low.seg'
highpath=query_quality_dir+'/query.predict.train.high.seg'
paths=[highpath,lowpath]
m1 = md5.new()
query0='downtown m'
query0='wanggl'
# query0='测试123'
query00=quote(query0)
print 'query00',query00,type(query00)
# sys.exit(2)
m1.update(query0)
md5id=m1.hexdigest()
# md5id='100'
print md5id
qualtiy0='1.1'
def es_update(query,md5id,weight):
    url = 'http://192.168.134.154:9090/meipai-query-suggest-api/updateOrInsert?query='+query+\
          '&id='+md5id+'&weight='+str(weight)
    print url
    req = urllib2.Request(url)
    req_res=urllib2.urlopen(req)
    res = req_res.read()
    print 'res=', res
es_update(query00,md5id,qualtiy0)
url = 'http://192.168.134.154:9090/meipai-query-suggest-api/query?query='+query00+'&uid='+md5id+'&from=0&size=20'
print url
req = urllib2.Request(url)
req_res=urllib2.urlopen(req)
res = req_res.read()
print 'res=', res
res_jo=json.loads(res)
statuscode=res_jo['status']
print 'statuscode',statuscode
url = 'http://192.168.134.154:9090/meipai-query-suggest-api/deleteById?id='+md5id
print url
req = urllib2.Request(url)
req_res=urllib2.urlopen(req)
res = req_res.read()
print 'res=', res
sys.exit(2)
sys.exit(2)
for i in range(len(paths)):
    curpath=paths[i]
    curfile=open(curpath)
    wc=0
    for line in curfile:
        wc+=1
        fields=line.split('\t')
        quality=float(fields[1])
        query=fields[0]
        m1.update(query)
        md5id=m1.hexdigest()
        if wc%10000==0:
            print wc
        print query,md5id,quality
        es_update(query,md5id,quality)
    curfile.close()