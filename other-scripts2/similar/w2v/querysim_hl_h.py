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
vectstrpath=datadir+'/query.w2v.vect.high'
vectstrfile=open(vectstrpath)
vectdict={}
simoutpath=datadir+'/query.sim.h'
print 'start read ',vectstrpath
wc=0
queryindexdict={}
syn0=None
index2word=[]
for line in vectstrfile.readlines():
    if wc==0:
        fields = line.strip().split(' ')
        vectlen = int(fields[0])
        hvectsize = int(fields[1])
        print 'vectlen=', vectlen, 'hvectsize=', hvectsize
        syn0 = np.zeros((vectlen, hvectsize))
        wc+=1
        continue
    fields=line.strip().split('\t')
    query=fields[0]
    query=unicode(query,'utf-8')
    vect=fields[2]
    norm=float(fields[3])
    vect=vect.split(',')
    vect=map(float,vect[:])
    syn0[wc-1]=vect
    index2word.append(query)
    wc+=1
    if wc%100000==0:
        print wc
    if wc>=vectlen:
        break
vectstrfile.close()
print 'read over:',vectstrpath
syn0norm=syn0/(np.sqrt((syn0[:]**2).sum(1))[..., None])
syn0norm_t=np.mat(syn0norm).T
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
        uquery = index2word[real_index]
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
for i in range(0,vectlen,step):
    if i==0:
        continue
    i_arr.append(i)
if i_arr[-1]!=vectlen-1:
    i_arr.append(vectlen-1)
for i in i_arr:
    tt0=time.time()
    print '==========','i', i, 'prei', prei
    syn0norm_t_b=syn0norm_t[:,prei:i]
    t0 = time.time()
    dists_all = np.dot(syn0norm, syn0norm_t_b)
    delta_t = time.time() - t0
    print 'dot time=', delta_t, delta_t / float(step)
    dists_all_copy=dists_all[:]
    thread.start_new_thread(aysn_file_flush,(dists_all_copy,prei))
    tt1=time.time()
    tt_delta=tt1-tt0
    print '======time=',tt_delta,tt_delta/float(i-prei)
    prei=i
simoutpathfile.close()
