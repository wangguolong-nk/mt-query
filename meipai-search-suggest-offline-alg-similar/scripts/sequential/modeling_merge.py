# -*- coding:utf-8 -*-
'''
增量更新模型，并得出query时序相似结果
输入：query.sequential.20170520及full.query.sequential.model及query.predict.train.high.seg
输出：full.query.sequential.model(更新)及full.query.sequential.view
'''
import json
import sys
import os

deploydir=None
deleteFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk == '-deploydir':
        deploydir = argv
    elif argk=="-delete":
        deleteFlag=int(argv)
if not deploydir:
    print 'need PARAMETER: deploydir!'
    sys.exit(1)
datadir=deploydir+'/work_data/days'
def writejson2file(data, filename):
    with open(filename, 'w+') as outfile:
        data = json.dumps(data, indent=4, sort_keys=True)
        outfile.write(data)
def readdatafromfile(filename):
    with open(filename) as outfile:
        return json.load(outfile)
json_data={}
deletepaths=[]
for root,dirs,filenames in os.walk(datadir):
    if root!=datadir:
       continue
    # print root,dirs,filenames
    for filename in filenames:
        if not filename.startswith('query.sequential.model'):
            continue
        cur_json = readdatafromfile(root+'/'+filename)
        deletepaths.append(root+'/'+filename)
        print '===',root+'/'+filename
        if not json_data:
            json_data=cur_json
        else:
            for key in cur_json:
                val1=cur_json[key]
                json_data.setdefault(key,{})
                val2=json_data[key]
                assert isinstance(val1,dict)
                assert isinstance(val2,dict)
                for key2 in val1:
                    val2.setdefault(key2,0.0)
                    val2[key2]=val2[key2]+val1[key2]
                json_data[key]=val2
outjsonpath=deploydir+'/work_data/full.query.sequential.model'
if os.path.exists(outjsonpath):
    pre_json=readdatafromfile(outjsonpath)
    for k1 in pre_json:

        json_data.setdefault(k1,{})
        k2_dict_cur=json_data[k1]
        # assert isinstance(k2_dict_cur,data)
        k2_dict_pre=pre_json[k1]
        # assert isinstance(k2_dict_pre,data)
        for k2 in k2_dict_pre:
            k2_dict_cur.setdefault(k2,0.0)
            k2_dict_cur[k2]+=k2_dict_pre[k2]
        json_data[k1]=k2_dict_cur

if deleteFlag==1:
    for path in deletepaths:
        os.remove(path)
else:
    import shutil
    for path in deletepaths:
        print 'move',path,datadir+'/pre'
        shutil.move(path,datadir+'/pre')
writejson2file(json_data,outjsonpath)

outviewpath=deploydir+'/work_data/full.query.sequential.view'
high_quality_query_path=deploydir+'/work_data/query.quality.predict.high'
query2uids={}
with open(high_quality_query_path) as fi:
    for line in fi.readlines():
        fields=line.strip().split('\t')
        query=fields[0]
        query=unicode(query,'utf-8')
        uidstrs=[]
        if len(fields)>=4:
            uidstrs=fields[3].split('|')
        query2uids[query]=uidstrs
with open(outviewpath,'w+') as outf:
    for pre_query in json_data:
        pre_query_dict=json_data[pre_query]
        if not pre_query_dict:
            continue
        assert isinstance(pre_query_dict,dict)
        pre_query_dict_sortlist=sorted(pre_query_dict.items(),key=lambda d:d[1],reverse=True)
        out_msg=pre_query+'\t'
        out_msg_arr=[]
        uid_set=set()
        for post_query,prob in pre_query_dict_sortlist:
            if post_query not in query2uids:
                continue
            try:
                assert isinstance(pre_query,unicode)
                assert isinstance(post_query,unicode)
                index_i=pre_query.index(post_query)
                if index_i>=0:
                    continue
            except Exception:
                pass
            uidarr=query2uids[post_query]
            for uid_i in uidarr:
                uid_set.add(uid_i)
            out_msg_arr.append(post_query+','+str(prob))
        if len(out_msg_arr)==0:
            continue
        out_msg+='|'.join(out_msg_arr)
        if len(uid_set)>0:
            out_msg+='\t'+'|'.join(uid_set)
        outf.write(out_msg.encode('utf-8')+'\n')