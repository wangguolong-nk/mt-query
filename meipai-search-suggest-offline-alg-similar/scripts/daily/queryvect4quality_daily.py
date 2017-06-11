# -*- coding:utf-8 -*-
'''
收集每天的query quality的vect
输入：24小时的query.2017052012.index及query.2017052012.vect
输出：query.vect.20170520
'''
import sys
import datetime
import os

projdatadir=None
quality_datadir=None
date=None
deleteFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-date':
        date=argv
    elif argk=='-projdatadir':
        projdatadir=argv
    elif argk=='-qualitydatadir':
        quality_datadir=argv
    elif argk=='-delete':
        deleteFlag=int(argv)
if not date or not projdatadir or not quality_datadir:
    print 'need PARAMETER: date and projdatadir!'
    sys.exit(1)
print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
date_arr=[]
hourdatadir=quality_datadir+'/work_data/hours'
query2vect_dict={}
deletepaths=[]
for i in range(24):
    hourstr=str(i)
    if len(hourstr)==1:
        hourstr='0'+hourstr
    cur_date=date+hourstr
    queryindex_path=hourdatadir+'/query.'+cur_date+'.index'
    queryvect_path=hourdatadir+'/query.'+cur_date+'.vect'
    if not os.path.exists(queryvect_path):
        continue
    deletepaths.append(queryindex_path)
    deletepaths.append(queryvect_path)
    index2query_dict={}
    with open(queryindex_path) as fi:
        for line in fi.readlines():
            fields=line.strip().split('\t')
            index=fields[0]
            query=fields[1]
            index2query_dict[index]=query
    with open(queryvect_path) as fi:
        for line in fi.readlines():
            fields=line.strip().split(',')
            vectstr=fields[1:]
            index=fields[0]
            query=index2query_dict[index]
            query2vect_dict[query]=vectstr
if len(query2vect_dict)==0:
    sys.exit(0)
outvectpath=projdatadir+'/work_data/train_data/query.vect.'+date
with open(outvectpath,'w+') as fo:
    for query in query2vect_dict:
        fo.write(','.join(query2vect_dict[query])+'\n')
if deleteFlag==1:
    for deletepath in deletepaths:
        os.remove(deletepath)
print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')