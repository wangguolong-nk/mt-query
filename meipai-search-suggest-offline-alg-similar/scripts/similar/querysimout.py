# -*- coding:utf-8 -*-
'''
输入：query.sim.delta,query.editdist.all,full.query.sequential.view
辅助：query.quality.predict.high，query.quality.predict.low
输出：query.h.sim.out, query.h.sim.out.simple, query.l.sim.out, query.l.sim.out.simple
'''
import sys
import re

from langconv import Converter

deploydir=None
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-deploydir':
        deploydir=argv
if not deploydir:
    print 'MUST NEED parameter: deploydir !'
    sys.exit(2)
datadir=deploydir+'/work_data'
query_predict_train_high_seg_path=datadir+'/query.quality.predict.high'
query_predict_train_low_seg_path=datadir+'/query.quality.predict.low'

high_query_dict={}
low_query_dict={}
# 转换繁体到简体
def cht_to_chs(line):
    line = Converter('zh-hans').convert(line)
    line=line.encode('utf-8')
    return line
query_predict_train_high_seg_pathfile = open(query_predict_train_high_seg_path)
query_predict_train_low_seg_pathfile = open(query_predict_train_low_seg_path)
querysegfiles = [query_predict_train_high_seg_pathfile, query_predict_train_low_seg_pathfile]
for i in range(2):
        querysegfile=querysegfiles[i]
        if i==0:
            cur_query_dict=high_query_dict
        else:
            cur_query_dict=low_query_dict
        for line in querysegfile.readlines():
            line=line.strip()
            fields=line.split('\t')
            query=fields[0]
            quality=float(fields[1])
            if len(fields)<3:
                continue
            res=fields[2]
            uids=''
            if len(fields)>=4:
                uids=fields[3]
            cur_query_dict[query]=(quality,res,uids)
        querysegfile.close()
# query_sim_h_path= datadir + '/query.sim.h'
# query_sim_l_path= datadir + '/query.sim.l'
query_sim_delta_path= datadir + '/query.sim.delta'

# new_query_sim_delta_path_f=open(new_query_sim_delta_path,'w+')
#query语义相似
query_sim_hl_dict={}
# process_w2v_simout(query_sim_h_path,query_sim_hl_dict,[high_query_dict])
# process_w2v_simout(query_sim_l_path,query_sim_hl_dict,[low_query_dict],sim_thr=0.5)
with open(query_sim_delta_path) as fi:
    for line in fi.readlines():
        fields=line.strip().split('\t')
        query=fields[0]
        sims=fields[1]
        query_sim_hl_dict[query]=sims
# process_w2v_simout(query_sim_delta_path, query_sim_hl_dict, [high_query_dict,low_query_dict])
print 'query_sim_hl_dict len=',len(query_sim_hl_dict)

def process_ed_simout(query_ed_path,query_ed_dict):
    query_ed_pathfile=open(query_ed_path)
    for line in query_ed_pathfile.readlines():
        fields = line.split('\t')
        query = fields[0]
        query_fields=query.split('(')
        query=query_fields[0]
        if len(fields)<2:
            print line
            print query
            print query_ed_path
            continue
        sims = fields[1]
        new_sims = re.split('\)\s', sims)
        wc_item=0
        sim_out=[]
        for item in new_sims:
            item_fields = item.split('(')
            query1 = item_fields[0]
            query1 = query1.strip()
            query1=query1.replace(' ','')
            if query1=='':
                continue
            sim_out.append(query1)
            wc_item+=1
            if wc_item>=4:
                break
        if len(sim_out)<1:
            continue
        sim_out=','.join(sim_out)
        query_ed_dict[query]=sim_out
    query_ed_pathfile.close()

#query编辑相似
query_ed_hl_dict={}
query_ed_path= datadir + '/query.editdist.all'
process_ed_simout(query_ed_path,query_ed_hl_dict)
print 'query_ed_hl_dict len=',len(query_ed_hl_dict)

def process_sequential_simout(sequential_outsimpath,query_sequential_dict,topN=5):
    with open(sequential_outsimpath) as fi:
        for line in fi.readlines():
            line=line.strip()
            fields=line.split('\t')
            if len(fields)<=1:
                continue
            query=fields[0]
            sims=fields[1]
            item_wc=0
            query_simout=[]
            for item in sims.split('|'):
                item_query=item.split(',')[0]
                query_simout.append(item_query)
                item_wc+=1
                if item_wc>=topN:
                    break
            if len(query_simout)<1:
                continue
            query_simout=','.join(query_simout)
            query_sequential_dict[query]=query_simout

#query时序相似
query_sequential_hl_dict={}
sequential_outsimpath=datadir+'/full.query.sequential.view'
process_sequential_simout(sequential_outsimpath,query_sequential_hl_dict)
print 'query_sequential_hl_dict len=',len(query_sequential_hl_dict)


outhsimfile= open(datadir + '/query.h.sim.out','w+')
s_outhsimfile= open(datadir + '/query.h.sim.out.simple','w+')
outlsimfile= open(datadir + '/query.l.sim.out','w+')
s_outlsimfile= open(datadir + '/query.l.sim.out.simple','w+')
outsimfile_arr=[outhsimfile,s_outhsimfile,outlsimfile,s_outlsimfile]

query_predict_train_high_seg_pathfile=open(query_predict_train_high_seg_path)
query_predict_train_low_seg_pathfile=open(query_predict_train_low_seg_path)
outfiles=[query_predict_train_high_seg_pathfile,query_predict_train_low_seg_pathfile]

for i in range(2):
    startIndex=i*2
    curoutfile=outfiles[i]
    curoutsimfile=outsimfile_arr[startIndex]
    s_curoutsimfile=outsimfile_arr[startIndex+1]
    if i==0:#high
        cur_query_dict=high_query_dict
    else:#low
        cur_query_dict=low_query_dict
    testi = 0
    for line in curoutfile.readlines():
        line=line.strip()
        fields=line.split('\t')
        query=fields[0]
        testi += 1
        query_out_msg=query+'\t'+fields[1]+'\t'
        temp_ok_flag=False
        sim_uid_set=set()
        if query in query_sim_hl_dict:#语义相似
            w2vsimout=query_sim_hl_dict[query]
            query_out_msg+=" 语义相似："+w2vsimout+'\t'
            temp_ok_flag=True
            for w2vsimout_item in w2vsimout.split(','):
                if w2vsimout_item in cur_query_dict:
                    val=cur_query_dict[w2vsimout_item]
                    uids_temp=val[2]
                    if uids_temp!='':
                        for uid_temp in uids_temp.split('|'):
                            sim_uid_set.add(uid_temp)
        if query in query_ed_hl_dict:#编辑相似
            ed_simout=query_ed_hl_dict[query]
            query_out_msg+=" 编辑相似："+ed_simout
            temp_ok_flag=True
            for ed_simout_item in ed_simout.split(','):
                if ed_simout_item in cur_query_dict:
                    val = cur_query_dict[ed_simout_item]
                    uids_temp = val[2]
                    if uids_temp != '':
                        for uid_temp in uids_temp.split('|'):
                            sim_uid_set.add(uid_temp)
        if query in query_sequential_hl_dict:#时序相似
            sequential_simout=query_sequential_hl_dict[query]
            query_out_msg+=" 时序相似："+sequential_simout
            for sequential_simout_item in sequential_simout.split(','):
                if sequential_simout_item in cur_query_dict:
                    val=cur_query_dict[sequential_simout_item]
                    uids_temp = val[2]
                    if uids_temp != '':
                        for uid_temp in uids_temp.split('|'):
                            sim_uid_set.add(uid_temp)
        uids = ''
        if len(fields) >= 4:
            uids = fields[3]
        if uids!='':
            if len(sim_uid_set)!=0:
                uids+='|'
        if uids!='' or len(sim_uid_set)!=0:
            query_out_msg+=' 相似用户名uid：'+uids+'|'.join(sim_uid_set)
        if temp_ok_flag:
            s_curoutsimfile.write(query_out_msg+'\n')
        curoutsimfile.write(query_out_msg+'\n')
    curoutfile.close()
    curoutsimfile.close()
    s_curoutsimfile.close()