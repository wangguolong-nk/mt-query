# -*- coding:utf-8 -*-
'''
昨天的w2v相似结果整合更新所有的w2v相似
w2v相似增量更新
输入：query.sim.20170520
输出：query.sim.delta.full(更新，达到阀值的所有相似结果), query.sim.delta(更新,提取相似结果),query.sim.20170520(删除)
'''
import sys
import re
import os


def process_w2v_simout(simpath,query_sim_dict,query_sim_full_dict,query_dict_arr,sim_thr=0.6):
    simpathfile=open(simpath)
    print simpath
    wc = 0
    for line in simpathfile.readlines():
        wc += 1
        if wc % 100000 == 0 and wc != 0:
            print wc
        fields = line.split('\t')
        # print line
        query = fields[0]
        # print query
        query_val=None
        for qd in query_dict_arr:
            if query in qd:
                query_val=qd[query]
        if not query_val:
            continue
        seg_str=query_val[1]
        seg_arr=seg_str.split(' ')
        if len(fields)<2:
            continue
        sims = fields[1]
        query_sim_full_dict[query]=sims
        new_sims = re.split('\)\s', sims)
        fulled_set = []
        part_same_set = []
        same_no_set = []

        seg_set = set()
        seg_set.add(seg_str)
        for item in new_sims:
            item=item.strip()
            if item=='':
                continue
            item_fields = item.split('(')
            query1 = item_fields[0]
            query1 = query1.strip()
            query1 = query1.replace(' ', '')
            if query1=='':
                continue
            query1_simval=item_fields[1]
            query1_simval=float(query1_simval)
            if wc<2:
                print query1_simval,query1
            if query1_simval<sim_thr:
                continue
            query1_val=None
            for qd in query_dict_arr:
                if query1 in qd:
                    query1_val = qd[query1]
            if not query1_val:
                continue
            seg1_str=query1_val[1]
            seg1_arr=seg1_str.split(' ')
            if seg1_str in seg_set:
                continue
            seg_set.add(seg1_str)
            try:
                query_score = float(item_fields[1])
            except Exception:
                query_score = float(item_fields[1][:-1])
            if query_score == 1:
                continue
            same_wc_left = 0
            for s in seg_arr:
                if s in seg1_arr:
                    same_wc_left += 1
            if same_wc_left == len(seg1_arr):
                continue
            if same_wc_left == len(seg_arr):
                if len(fulled_set) <= 4:
                    fulled_set.append(query1)
            elif same_wc_left > 0:
                if len(part_same_set) <= 5:
                    part_same_set.append(query1)
            else:
                if len(same_no_set) < 3:
                    same_no_set.append(query1)
        sim_out_arr = fulled_set + part_same_set + same_no_set
        if len(sim_out_arr) == 0:
            continue
        query_sim_dict[query]=','.join(sim_out_arr)
    simpathfile.close()

date=None
deploydir=None
deleteFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    if argk=='-date':
        date=argv
    elif argk=='-deploydir':
        deploydir=argv
    elif argk=='-delete':
        deleteFlag=int(argv)

datepath=deploydir+'/work_data/days/query.sim.'+date
# outpath=deploydir+'/work_data/days/query.simout.delta.'+date
w2v_delta_simoutpath=deploydir+'/work_data/query.sim.delta'
w2v_full_simoutpath=deploydir+'/work_data/query.sim.delta.full'

high_query_dict={}
low_query_dict={}

print 'read query.quality.predict.high query.quality.predict.low ...'
query_predict_train_high_seg_pathfile = open(deploydir+'/work_data/query.quality.predict.high')
query_predict_train_low_seg_pathfile = open(deploydir+'/work_data/query.quality.predict.low')
querysegfiles = [query_predict_train_high_seg_pathfile, query_predict_train_low_seg_pathfile]
for i in range(2):
        querysegfile = querysegfiles[i]
        if i == 0:
            cur_query_dict = high_query_dict
        else:
            cur_query_dict = low_query_dict
        for line in querysegfile.readlines():
            line = line.strip()
            fields = line.split('\t')
            query = fields[0]
            quality = float(fields[1])
            if len(fields) < 3:
                continue
            res = fields[2]
            uids = ''
            if len(fields) >= 4:
                uids = fields[3]
            cur_query_dict[query] = (quality, res, uids)
        querysegfile.close()

query_sim_hl_dict={}
query_sim_full_dict={}
with open(w2v_delta_simoutpath) as fi:
    for line in fi.readlines():
        fields=line.strip().split('\t')
        query=fields[0]
        sims=fields[1]
        query_sim_hl_dict[query]=sims
with open(w2v_full_simoutpath) as fi:
    for line in fi.readlines():
        fields=line.strip().split('\t')
        query=fields[0]
        if len(fields)<2:
            continue
        sims=fields[1]
        query_sim_full_dict[query]=sims

print 'process_w2v_simout...'
process_w2v_simout(datepath,query_sim_hl_dict,query_sim_full_dict,[high_query_dict,low_query_dict])

print 'write ...'
outfile=open(w2v_delta_simoutpath+'.del','w+')
for query in query_sim_hl_dict:
    sims = query_sim_hl_dict[query]
    outfile.write(query+'\t'+sims+'\n')
outfile.close()
os.remove(w2v_delta_simoutpath)
os.rename(w2v_delta_simoutpath+'.del',w2v_delta_simoutpath)

outfullfile=open(w2v_full_simoutpath+'.del','w+')
for query in query_sim_full_dict:
    sims=query_sim_full_dict[query]
    outfullfile.write(query+'\t'+sims+'\n')
outfullfile.close()
os.remove(w2v_full_simoutpath)
os.rename(w2v_full_simoutpath+'.del',w2v_full_simoutpath)

if deleteFlag==1:
    os.remove(datepath)
print 'SUCCESS'