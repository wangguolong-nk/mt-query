# -*- coding:utf-8 -*-
import sys
import os

deploy_datadir='/data1/wgl/deploy_data'

if 1==1:
        date=sys.argv[1]
        query_editdist_all_delta_path=deploy_datadir+'/work_data/search/delta_data/query.editdist.all.delta'
        delta_editdist_outpath=deploy_datadir+'/work_data/search/delta_data/query.editdist.'+date
        paths=[]
        paths.append(query_editdist_all_delta_path)
        paths.append(delta_editdist_outpath)
        query2ed_dict = {}
        for ed_path in paths:
            with open(ed_path) as fi:
                print ed_path
                for line in fi.readlines():
                    line = line.strip()
                    fields = line.split('\t')
                    query = fields[0]
                    query_fields = query.split('(')
                    query = query_fields[0]
                    if len(fields) < 2:
                        continue
                    query2ed_dict[query] = line
        outpath = query_editdist_all_delta_path+'.del'
        with open(outpath, 'w+') as fo:
            for query in query2ed_dict:
                edline = query2ed_dict[query]
                fo.write(edline + '\n')
        os.remove(query_editdist_all_delta_path)
        os.rename(outpath,query_editdist_all_delta_path)
        if 1==1:
            print 'delete:',delta_editdist_outpath
            os.rename(delta_editdist_outpath,delta_editdist_outpath+'.del')

sys.exit(0)

dates=None
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-deploy_datadir':
        deploy_datadir=argv
    elif argk=='-dates':
        dates=argv
if not deploy_datadir or not dates:
    print 'MUST NEED parameter: deploy_datadir and dates!'
    sys.exit(2)
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr
print 'date_arr',date_arr
delta_dir=deploy_datadir+'/work_data/search/delta_data'
paths=[]
paths.append(delta_dir+'/query.editdist.full.lh')
paths.append(delta_dir+'/query.editdist.full.hh')
for date in date_arr:
    cur_ed_path=delta_dir+'/query.editdist.'+date
    paths.append(cur_ed_path)
query2ed_dict={}
for ed_path in paths:
    with open(ed_path) as fi:
        print ed_path
        for line in fi.readlines():
            line=line.strip()
            fields = line.split('\t')
            query = fields[0]
            query_fields = query.split('(')
            query = query_fields[0]
            if len(fields) < 2:
                continue
            query2ed_dict[query]=line
outpath=delta_dir+'/query.editdist.all.delta'
with open(outpath,'w+') as fo:
    for query in query2ed_dict:
        edline=query2ed_dict[query]
        fo.write(edline+'\n')