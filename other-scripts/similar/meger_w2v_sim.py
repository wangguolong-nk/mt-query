# -*- coding:utf-8 -*-
import re

datadir='/data1/wgl/meipai-search-suggest-offline-alg-data/similar/work_data'
query_sim_h_path= datadir + '/query.sim.h'
query_sim_l_path= datadir + '/query.sim.l'
query_sim_delta_path= datadir + '/query.sim.delta'
view_query_sim_delta_path=datadir+'/query.sim.delta.new'#out
query_predict_train_high_seg_path=datadir+'/query.quality.predict.high'
query_predict_train_low_seg_path=datadir+'/query.quality.predict.low'
high_query_dict={}
low_query_dict={}
if 1==1:
    query_predict_train_high_seg_pathfile = open(query_predict_train_high_seg_path)
    query_predict_train_low_seg_pathfile = open(query_predict_train_low_seg_path)
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
pass
def process_w2v_simout(simpath,query_sim_dict,query_dict_arr,sim_thr=0.6):
    simpathfile=open(simpath)
    print simpath
    wc = 0
    for line in simpathfile.readlines():
        wc += 1
        if wc % 100000 == 0 and wc != 0:
            print wc
        fields = line.split('\t')
        query = fields[0]#被相似的query，看看是不是在相应的高低质量query当中，其实就是判断该query是否有相应的质量
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
            if same_wc_left == len(seg1_arr):#qi分词全在q集合中
                continue
            if same_wc_left == len(seg_arr):#q分词全在qi集合中
                if len(fulled_set) <= 4:
                    fulled_set.append(query1)
            elif same_wc_left > 0:#部分相同
                if len(part_same_set) <= 5:
                    part_same_set.append(query1)
            else:  # same_wc_left==0#完全不同，字面形式
                if len(same_no_set) < 3:
                    same_no_set.append(query1)
        sim_out_arr = fulled_set + part_same_set + same_no_set
        if len(sim_out_arr) == 0:
            continue
        query_sim_dict[query]=','.join(sim_out_arr)
    simpathfile.close()
pass
new_query_sim_delta_path_f=open(view_query_sim_delta_path,'w+')
#query语义相似
query_sim_hl_dict={}
process_w2v_simout(query_sim_h_path,query_sim_hl_dict,[high_query_dict])
process_w2v_simout(query_sim_l_path,query_sim_hl_dict,[low_query_dict],sim_thr=0.5)
process_w2v_simout(query_sim_delta_path, query_sim_hl_dict, [high_query_dict,low_query_dict])
print 'query_sim_hl_dict len=',len(query_sim_hl_dict)

# new_query_sim_delta_path= datadir + '/query.sim.delta.full'
# query_sim_hl_dict={}
# in_paths=[query_sim_h_path,query_sim_l_path,query_sim_delta_path]
# for in_path in in_paths:
#     with open(in_path) as fi:
#         for line in fi.readlines():
#             fields=line.strip().split('\t')
#             query=fields[0]
#             sims=fields[1]
#             query_sim_hl_dict[query]=sims
#
# new_query_sim_delta_path_f = open(new_query_sim_delta_path, 'w+')
for query in query_sim_hl_dict:
    sims=query_sim_hl_dict[query]
    new_query_sim_delta_path_f.write(query+'\t'+sims+'\n')
new_query_sim_delta_path_f.close()