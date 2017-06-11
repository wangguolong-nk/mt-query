# -*- coding:utf-8 -*-
'''
query的w2v向量结果当天的query列表，生成当天的query时序模型（有效的query转移概率：同一个uid产生某个概率，所有用户累加）
query.w2v.20170520.vect + query.seq.20170520 ==> query.sequential.model.20170520
'''

import json
import sys
import os
import numpy as np

dates=None
deploydir=None
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
    elif argk == '-deploydir':
        deploydir = argv
    elif argk=="-delete":
        deleteFlag=int(argv)
if not dates or not deploydir:
    print 'need PARAMETER: dates and deploydir!'
    sys.exit(1)
datadir=deploydir+'/work_data/days'
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr
print 'date_arr',date_arr
def writejson2file(data, filename):
    with open(filename, 'w+') as outfile:
        data = json.dumps(data, indent=4, sort_keys=True)
        outfile.write(data)
def readdatafromfile(filename):
    with open(filename) as outfile:
        return json.load(outfile)

date_name=date_arr[0]
if len(date_arr)>1:
    date_name+='-'+date_arr[-1]
sequential_model_json={}
query_sequential_model_path = datadir + '/query.sequential.model.'+date_name
if os.path.exists(query_sequential_model_path):
    sequential_model_json=readdatafromfile(query_sequential_model_path)
deleteFilepaths=[]
for date in date_arr:
    cur_vectpath=datadir+'/query.w2v.'+date+'.vect'
    cur_seqpath=datadir+'/query.seq.'+date
    deleteFilepaths.append(cur_vectpath)
    deleteFilepaths.append(cur_seqpath)
    query2vectdict={}
    with open(cur_vectpath) as vf:
        wc=0
        for line in vf.readlines():
            if wc==0:
                wc+=1
                continue
            fields=line.strip().split('\t')
            vectstr=fields[2]
            query=fields[0]
            query=unicode(query,'utf-8')
            normstr=fields[3]
            query2vectdict[query]=(vectstr,normstr)
            wc+=1
    with open(cur_seqpath) as sf:
        pre_uid=None
        query_arr=[]
        wc=0
        for line in sf.readlines():
            wc+=1
            if wc%100000==0:
                print wc
            fields=line.strip().split('\t')
            query=fields[1]
            query = unicode(query, 'utf-8')
            uid=fields[0]
            if not pre_uid:
                pre_uid=uid
                if query not in query_arr:
                    query_arr.append(query)
            else:
                if pre_uid==uid:
                    if query not in query_arr:
                        query_arr.append(query)
                    continue
                else:
                    if len(query_arr)>1:
                        query_vect_arr=[]
                        for item in query_arr:
                            if item not in query2vectdict:
                                continue
                            vectstr,normstr=query2vectdict[item]
                            vectstr_arr=vectstr.split(',')
                            vect_float_arr=map(float,vectstr_arr)
                            vect_np=np.array(vect_float_arr)
                            normstr=float(normstr)
                            normstr=np.sqrt(normstr)
                            query_vect_arr.append((item,vect_np,normstr))
                        query_vect_arr_len=len(query_vect_arr)
                        for i in range(query_vect_arr_len-1):
                            query_vect_arri=query_vect_arr[i]
                            vect_npi=query_vect_arri[1]
                            queryi=query_vect_arri[0]
                            normi=query_vect_arri[2]
                            sequential_model_json.setdefault(queryi,{})
                            j_json=sequential_model_json[queryi]
                            for j in range(i+1,query_vect_arr_len):
                                query_vect_arrj=query_vect_arr[j]
                                vect_npj=query_vect_arrj[1]
                                queryj=query_vect_arrj[0]
                                normj=query_vect_arrj[2]
                                molecule=np.dot(vect_npi,vect_npj)
                                cosval=molecule/(normi*normj)
                                if cosval<0.4:
                                    continue
                                j_json.setdefault(queryj,0)
                                j_json[queryj]+=cosval
                            sequential_model_json[queryi]=j_json

                    pre_uid=uid
                    query_arr=[query]
writejson2file(sequential_model_json,query_sequential_model_path)
if deleteFlag==1:
    for onepath in deleteFilepaths:
        os.remove(onepath)