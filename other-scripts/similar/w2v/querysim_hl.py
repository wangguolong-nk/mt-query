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
# datadir='/Users/zj-db0803/wgl_work/sim_work'
hvectstrpath= datadir + '/query.w2v.vect.high'
hvectstrfile=open(hvectstrpath)
lvectstrpath= datadir + '/query.w2v.vect.low'
lvectstrfile=open(lvectstrpath)
vectstrfiles=[hvectstrfile,lvectstrfile]
vectdict={}
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
queryindexdict={}
index2word=[]
word2indexdict={}
print 'start read ',hvectstrpath
hsyn0norm=None
for i in range(len(vectstrfiles)):
    simoutpath = datadir + '/query.sim'
    vectstrfile=vectstrfiles[i]
    if i==0:#high
        header=vectstrfile.readline()
        header=header.strip()
        fields=header.split(' ')
        hcounter=int(fields[0])
        hvectsize=int(fields[1])
        vectlen=hcounter
        hsyn0 = np.zeros((hcounter, hvectsize))
        print 'high=',header,hsyn0.shape
        simoutpath=simoutpath+'.h'
    elif i==1:#low
        header = vectstrfile.readline()
        header=header.strip()
        fields = header.split(' ')
        lcounter = int(fields[0])
        lvectsize = int(fields[1])
        assert hvectsize==lvectsize
        lsyn0 = np.zeros((lcounter, lvectsize))
        print 'low=', header,lsyn0.shape
        vectlen=lcounter
        simoutpath=simoutpath+'.l'
    else:
        break
    wc = 0

    for line in vectstrfile.readlines():
        fields=line.strip().split('\t')
        query=fields[0]
        query=unicode(query,'utf-8')
        vect=fields[2]
        norm=float(fields[3])
        vect=vect.split(',')
        # assert len(vect)==200
        vect=map(float,vect[:])
        if wc>3500:
            break
        if i==0:
            hsyn0[wc]=vect
        else:
            lsyn0[wc]=vect
        index2word.append(query)
        word2indexdict[query]=wc
        wc+=1
        if wc%100000==0:
            print wc
        if wc>=vectlen:
            break
    vectstrfile.close()
    # print 'read over:',hvectstrpath
    if i==0:
        hsyn0norm=hsyn0/(np.sqrt((hsyn0[:]**2).sum(1))[..., None])
        hsyn0norm_t=np.mat(hsyn0norm).T
        hsyn0norm_t = hsyn0norm_t.getA()
        print 'hsyn0norm_t', hsyn0norm_t.shape, type(hsyn0norm_t)
        syn0norm_t=hsyn0norm_t
    else:
        lsyn0norm = lsyn0 / (np.sqrt((lsyn0[:] ** 2).sum(1))[..., None])
        lsyn0norm_t=np.mat(lsyn0norm).T
        lsyn0norm_t = lsyn0norm_t.getA()
        print 'lsyn0norm_t',lsyn0norm_t.shape,type(lsyn0norm_t)
        syn0norm_t=lsyn0norm_t
    step=1000
    # step=2
    simoutpathfile=open(simoutpath,'w+')
    mutex=threading.Lock()
    i_arr=[]
    for col in range(0,vectlen,step):
        if col==0:
            continue
        i_arr.append(col)
    if i_arr[-1]!=vectlen-1:
        i_arr.append(vectlen-1)
    prej = 0
    for j in i_arr:
        tt0=time.time()
        print '==========','j', j, 'prej', prej
        syn0norm_t_b=syn0norm_t[:,prej:j]
        t0 = time.time()
        print 'dot',hsyn0norm.shape,syn0norm_t_b.shape
        dists_all = np.dot(hsyn0norm, syn0norm_t_b)
        delta_t = time.time() - t0
        print 'dot time=', delta_t, delta_t / float(step)
        dists_all_copy=dists_all[:]
        thread.start_new_thread(aysn_file_flush,(dists_all_copy,prej))
        tt1=time.time()
        tt_delta=tt1-tt0
        print '======time=',tt_delta,tt_delta/float(j-prej)
        prej=j
    simoutpathfile.close()
