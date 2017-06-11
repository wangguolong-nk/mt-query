# -*- coding:utf-8 -*-
import jieba
from gensim.models import Word2Vec
import sys
import datetime
import numpy as np
import codecs

projdatadir=None
dates=None
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-dates':
        dates=argv
    elif argk=='-projdatadir':
        projdatadir=argv
if not dates or not projdatadir:
    print 'need PARAMETER: dates and projdatadir!'
    sys.exit(1)
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr
print 'date_arr',date_arr
datadir=projdatadir+'/work_data/search/delta_data'
stopwordspath=projdatadir+'/data/stopwords.dat'
w2v_model_vectors_path=projdatadir+'/work_data/similar/w2v/vectors.bin.view'
print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

stoplist={}.fromkeys([line.strip() for line in codecs.open(stopwordspath,'r','utf-8')])
print 'start load Word2Vec model:',w2v_model_vectors_path
model=Word2Vec.load_word2vec_format(w2v_model_vectors_path,binary=False,unicode_errors='ignore')
print 'load over!'
def cut_query(query):
    cut_list=jieba.cut(query)
    cut_list=[word.strip() for word in cut_list if word not in stoplist and word.strip()!='']
    return cut_list
def norm(vect):
    norm_val=0.0
    for a in vect:
        norm_val += a ** 2
    return norm_val
for date in date_arr:
    query_path=datadir+'/query.'+date
    query_pathfile=open(query_path)
    print query_path
    queryvectoutpath=datadir+'/query.w2v.'+date+'.vect'
    query_vect_file=open(queryvectoutpath+'.del','w+')
    wc=0
    for query in query_pathfile.readlines():
        query=query.strip()
        cut_list=cut_query(query)
        res=' '.join(cut_list)
        res=res.encode('utf-8')
        vect=np.zeros(200)
        flag=True
        for uword in cut_list:
            if uword in model:
                vect=vect+model[uword]
            else:
                flag=False
                break
        if not flag:
            continue
        res = ' '.join(cut_list)
        if len(res)<=1:
            continue
        res = res.encode('utf-8')
        norm_val=norm(vect)
        vectstr = ','.join([str(v) for v in vect.tolist()])
        query_vect_file.write(query+'\t'+res+'\t'+vectstr+'\t'+str(norm_val)+'\n')
        wc += 1
        if wc%100000==0:
            print wc
            query_vect_file.flush()
    query_vect_file.close()
    r_query_vect_file=open(queryvectoutpath,'w+')
    query_vect_file=open(queryvectoutpath+'.del')
    r_query_vect_file.write(str(wc)+' 200'+'\n')
    for line in query_vect_file.readlines():
        r_query_vect_file.write(line)
    r_query_vect_file.close()
    query_vect_file.close()
    import os
    os.remove(queryvectoutpath+'.del')
print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
