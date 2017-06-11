# -*- coding:utf-8
'''
query.w2v.vect.high维护：
当天的query.w2v.20170520.vect中,通过query.predict.train.high.seg及query.predict.train.low.seg，分为低query和高query
其中低query直接把query.w2v.vect.high相应的query去除,高质量query直接加上

query.w2v.20170520.vect在query.w2v.vect.high中寻找相似query
生成query.sim.20170520

输入：query.w2v.vect.high(时序还要用)，query.quality.predict.high，query.quality.predict.low, query.w2v.20170520.vect
输出：query.w2v.vect.high（更新）,query.sim.20170520
'''

import numpy as np
import datetime
import time
import sys
from gensim import matutils
import thread
import threading

dates=None
projdatadir=None
deleteFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-dates':
        dates=argv
    elif argk == '-projdatadir':
        projdatadir = argv
    elif argk=='-delete':
        deleteFlag=int(argv)
if not dates or not projdatadir:
    print 'need PARAMETER: dates and projdatadir!'
    sys.exit(1)
datadir=projdatadir+'/work_data'
deltadir=datadir+'/days'
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr

def cos(v1, v2, normA_power, normB_power):
        try:
            dot_product = 0.0
            for a, b in zip(v1, v2):
                dot_product += a * b
            return dot_product / (normA_power * normB_power)
        except Exception, e:
            print e.message
            return None
print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#query.w2v.vect.high维护开始
high_query_quality=datadir+'/query.quality.predict.high'
low_query_quality=datadir+'/query.quality.predict.low'
high_query_qualityfile=open(high_query_quality)
high_query_quality_dict={}
low_query_quality_dict={}
for line in high_query_qualityfile.readlines():
    fields=line.strip().split('\t')
    query=fields[0]
    quality=float(fields[1])
    high_query_quality_dict[query]=quality
high_query_qualityfile.close()
low_query_qualityfile=open(low_query_quality)
for line in low_query_qualityfile.readlines():
    fields=line.strip().split('\t')
    query=fields[0]
    quality=float(fields[1])
    low_query_quality_dict[query]=quality
low_query_qualityfile.close()
hvectstrpath=datadir+'/query.w2v.vect.high'#高质量的query向量，需要被更新

assert len(date_arr)==1
hquery2vectdict={}
lquery2vectdict={}
for date in date_arr:
    lvectstrpath= deltadir + '/query.w2v.'+date+'.vect'
    with open(lvectstrpath) as fi:
        wc=0
        for line in fi.readlines():
            wc+=1
            if wc==1:
                continue
            line=line.strip()
            fields=line.split('\t')
            query=fields[0]
            seg=fields[1]
            vectstr=fields[2]
            if query in high_query_quality_dict:
                hquery2vectdict[query]=line
            elif query in low_query_quality_dict:
                lquery2vectdict[query]=line
del low_query_quality_dict
del high_query_quality_dict
hvectstrpath_del=hvectstrpath+'.del'

wc=0
with open(hvectstrpath_del,'w+') as fo:
    wc_fi = 0
    with open(hvectstrpath) as fi:
        for line in fi:
            wc_fi+=1
            if wc_fi==1:
                continue
            line=line.strip()
            fields = line.split('\t')
            query = fields[0]
            if query in hquery2vectdict:
                continue
            elif query in lquery2vectdict:
                continue
            else:
                fo.write(line+'\n')
                wc+=1
    for query in hquery2vectdict:
        fo.write(hquery2vectdict[query] + '\n')
        wc += 1
with open(hvectstrpath_del) as fi:
    with open(hvectstrpath,'w+') as fo:
        fo.write(str(wc)+' 200'+'\n')
        for line in fi.readlines():
            fo.write(line)
import os
os.remove(hvectstrpath_del)
#query.w2v.vect.high维护结束
hvectstrfile=open(hvectstrpath)
queryindexdict={}
hsyn0=None
index2word=[]
word2indexdict={}
wc=0
print 'start read ',hvectstrpath
for line in hvectstrfile.readlines():
    if wc==0:
        fields = line.strip().split(' ')
        hcounter = int(fields[0])
        hvectsize = int(fields[1])
        print 'hcounter=',hcounter,'hvectsize=',hvectsize
        hsyn0 = np.zeros((hcounter, hvectsize))
        wc+=1
        continue
    fields=line.strip().split('\t')
    query=fields[0]
    query=unicode(query,'utf-8')
    if len(fields)<3:
        print line
        sys.exit(2)
    vect=fields[2]
    vect=vect.split(',')
    vect=map(float,vect[:])
    hsyn0[wc-1]=vect
    index2word.append(query)
    word2indexdict[query]=wc
    wc+=1
    if wc%100000==0:
        print wc
hvectstrfile.close()
print 'read over:',hvectstrpath

##############################
assert len(date_arr)==1
deletepath=[]
for date in date_arr:
    lvectstrpath= deltadir + '/query.w2v.'+date+'.vect'
    simoutpath=deltadir+'/query.sim.'+date
    deletepath.append(lvectstrpath)
    print 'start read ',lvectstrpath
    lsyn0=None
    lindex2word=[]
    wc=0
    lvectstrfile=open(lvectstrpath)
    for line in lvectstrfile.readlines():
        if wc==0:
            fields = line.strip().split(' ')
            lcounter = int(fields[0])
            lvectsize = int(fields[1])
            print 'lcounter=',lcounter,'lvectsize=',lvectsize
            lsyn0 = np.zeros((lcounter, lvectsize))
            wc+=1
            continue
        fields=line.strip().split('\t')
        query=fields[0]
        query=unicode(query,'utf-8')
        vect=fields[2]
        vect=vect.split(',')
        vect=map(float,vect[:])
        lsyn0[wc-1]=vect
        lindex2word.append(query)
        wc+=1
        if wc%100000==0:
            print wc
    lvectstrfile.close()
    print 'read over:',lvectstrpath

hsyn0norm= hsyn0 / (np.sqrt((hsyn0[:] ** 2).sum(1))[..., None])
lsyn0norm= lsyn0 / (np.sqrt((lsyn0[:] ** 2).sum(1))[..., None])
syn0norm_t=np.mat(lsyn0norm).T
syn0norm_t=syn0norm_t.getA()
print 'syn0norm_t',syn0norm_t.shape,type(syn0norm_t)
prei=0
step=1000
simoutpathfile=open(simoutpath,'w+')
mutex=threading.Lock()
over_prei_arr=[]
def aysn_file_flush(dists_all_temp,prei_temp):
    sim_num = dists_all_temp.shape[1]
    line_msgs=''
    for j in range(sim_num):
        real_index = prei_temp + j
        dists = dists_all_temp[:, j]
        uquery = lindex2word[real_index]
        best = matutils.argsort(dists,100, reverse=True)
        bestwords = [index2word[simindex]+'('+str(dists[simindex])+')' for simindex in best if
                     simindex != real_index and dists[simindex] >= 0.5]
        if len(bestwords) == 0:
            continue
        line_msg = uquery + '\t' + ' '.join(bestwords)
        line_msgs+=line_msg+'\n'
    mutex.acquire()
    simoutpathfile.write(line_msgs.encode('utf-8'))
    simoutpathfile.flush()
    over_prei_arr.append(prei_temp)
    mutex.release()
i_arr=[]
for i in range(0,lcounter,step):
    if i==0:
        continue
    i_arr.append(i)
if i_arr[-1]!=lcounter-1:
    i_arr.append(lcounter-1)
for i in i_arr:
    tt0=time.time()
    print '==========','i', i, 'prei', prei
    syn0norm_t_b=syn0norm_t[:,prei:i]
    t0 = time.time()
    print 'dot',hsyn0norm.shape,syn0norm_t_b.shape
    dists_all = np.dot(hsyn0norm, syn0norm_t_b)
    delta_t = time.time() - t0
    print 'dot time=', delta_t, delta_t / float(step)
    dists_all_copy=dists_all[:]
    thread.start_new_thread(aysn_file_flush,(dists_all_copy,prei))
    tt1=time.time()
    tt_delta=tt1-tt0
    print '======time=',tt_delta,tt_delta/float(i-prei)
    print 'len(over_prei_arr)=',len(over_prei_arr),'len(i_arr)=',len(i_arr)
    prei=i
while len(over_prei_arr)!=len(i_arr):
    pass
simoutpathfile.close()
if deleteFlag==1:
    for onepath in deletepath:
        os.remove(onepath)