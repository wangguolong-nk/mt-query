# -*- coding:utf-8 -*-
import numpy as np
import datetime
import time
import sys
from gensim import matutils
import thread
import threading

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
datadir='/data1/wgl/deploy_data/work_data/search/queryvect'
hvectstrpath= datadir + '/query.w2v.vect.high'
lvectstrpath= datadir + '/query.w2v.vect.low'
hvectstrfile=open(hvectstrpath)
simoutpath=datadir+'/query.sim.l'
print 'start read ',hvectstrpath
wc=0
queryindexdict={}
hsyn0=None
index2word=[]
word2indexdict={}
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

print 'start read ',lvectstrpath
lsyn0=None
lindex2word=[]
lword2indexdict={}
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
    lword2indexdict[query]=wc
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
    prei=i
simoutpathfile.close()
