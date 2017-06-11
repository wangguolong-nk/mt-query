# -*- coding:utf-8 -*-
import numpy as np
import pickle
import sys
from sklearn import preprocessing

dates=None
datadir=None
modeldir=None
mode=1
delta=0
deleteFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    if argk=='-dates':
        dates=argv
    elif argk=='-datadir':
        datadir=argv
    elif argk=='-mode':
        mode=int(argv)
    elif argk == '-delta':
        delta = int(argv)
    elif argk=='-modeldir':
        modeldir=argv
    elif argk=='-delete':
        deleteFlag=int(argv)
if not dates or not datadir or not modeldir:
    print 'need PARAMETER: dates and datadir and modeldir!'
    sys.exit(1)
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(i)
        date_arr=new_date_arr
if mode==1:#StandardScaler
    suffix = '.stand'
    # scaler=preprocessing.StandardScaler()
    scalerpath = modeldir+'/model.scaler.pickle.stand'
    print 'scalerpath=',scalerpath
    f=open(scalerpath, 'rb')
    scaler = pickle.load(f)
    f.close()
    print 'scaler.std_=', scaler.std_, type(scaler.std_)
    print 'scaler.mean_=', scaler.mean_, type(scaler.mean_)
elif mode==2:#MinMaxScaler
    suffix='.minmax'
    scaler = preprocessing.MinMaxScaler()
else:
    sys.exit(1)

modelpath=modeldir+'/model.lr.pickle'+suffix
with open(modelpath,'rb') as f:
    model=pickle.load(f)
print modelpath,' pickle.load over.'
print 'date_arr=',date_arr
deletepaths=[]
for date in date_arr:
    grobalid = 0
    id_prob_dict = {}
    id_line_dict = {}
    query_set = set()
    date=str(date)
    testvectpath= datadir + '/query.' + date + '.vect'
    deletepaths.append(testvectpath)
    testf=open(testvectpath)
    print testvectpath
    testset=np.loadtxt(testf,delimiter=',')
    testf.close()
    testset=testset[:,1:7]
    # print testset
    # print 'testset=',testset.shape,type(testset)

    X_train=scaler.transform(testset)

    testX=X_train
    # print 'testset=',testset
    predicted=model.predict_proba(testX)
    # print 'predicted=',predicted
    answer=predicted[:,1]
    testf=open(testvectpath)
    query_index=open(datadir+'/query.'+date+'.index')
    deletepaths.append(query_index)
    indexdict={}
    for line in query_index.readlines():
        fields=line.strip().split('\t')
        indexdict[fields[0]]=(fields[1],fields[2],fields[3])
    query_index.close()
    wc=-1
    for line in testf.readlines():
        wc+=1
        line=line.strip()
        if wc<=2:
            print 'line=',line
        fields=line.split(',')#len fields=14
        queryid=fields[0]
        if queryid not in indexdict:
            print 'queryid not in indexdict:',queryid
            continue
        query_val=indexdict[queryid]
        query_w=query_val[0]
        query_nickname=query_val[1]
        seg0str=query_val[2]
        if query_w in query_set:
            continue
        query_set.add(query_w)
        msgline=line+'\t'+str(answer[wc])+'\t'+query_w+'\t'+query_nickname+'\t'+seg0str
        prob = answer[wc]
        id_prob_dict[grobalid]=prob
        id_line_dict[grobalid]=msgline
        grobalid+=1
    testf.close()
    indexdict.clear()
    id_prob_dict_sorted_list=sorted(id_prob_dict.items(),key=lambda d:d[1],reverse=True)
    outfile=None
    if delta==1:
        quality_sorted_all_filepath=datadir+'/query.'+date+'.predict.q'+suffix
        print 'write ...',quality_sorted_all_filepath
        outfile=open(quality_sorted_all_filepath,'w+')
    else:
        quality_sorted_high_filepath=datadir+'/query.'+date+'.predict.q'+suffix+'.h'
        print 'write ...',quality_sorted_high_filepath
        outfile=open(quality_sorted_high_filepath,'w+')
    for (item_id,item_prob) in id_prob_dict_sorted_list:
        if item_prob>0.5 and delta==0:
            msgline = id_line_dict[item_id]
            outfile.write(msgline+'\n')
        elif delta==1:
            msgline = id_line_dict[item_id]
            outfile.write(msgline + '\n')
    outfile.close()
if deleteFlag==1:
    import os
    for delpath in deletepaths:
        os.rename(delpath)