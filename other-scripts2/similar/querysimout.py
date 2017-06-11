# -*- coding:utf-8 -*-
import sys
import re
import jieba
import codecs

from langconv import Converter

deploy_datadir=None
segFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-deploy_datadir':
        deploy_datadir=argv
    elif argk=='-seg':
        segFlag=int(argv)
if not deploy_datadir:
    print 'MUST NEED parameter: deploy_datadir !'
    sys.exit(2)
queryqualitydir=deploy_datadir+'/work_data/search'
deltadatadir=deploy_datadir+'/work_data/search/delta_data'
sequential_dir=deploy_datadir+'/work_data/sequential/daily'
stopwordspath=deploy_datadir+'/data/stopwords.dat'
stoplist={}.fromkeys([line.strip() for line in codecs.open(stopwordspath,'r','utf-8')])
def cut_query(query):
    cut_list=jieba.cut(query)
    cut_list=[word.strip() for word in cut_list if word not in stoplist and word.strip()!='']
    return cut_list
query_predict_train_high_path=queryqualitydir+'/query.predict.train.high'
query_predict_train_high_seg_path=queryqualitydir+'/query.predict.train.high.seg'
query_predict_train_low_path=queryqualitydir+'/query.predict.train.low'
query_predict_train_low_seg_path=queryqualitydir+'/query.predict.train.low.seg'

high_query_dict={}
low_query_dict={}
# 转换繁体到简体
def cht_to_chs(line):
    line = Converter('zh-hans').convert(line)
    line=line.encode('utf-8')
    return line
if segFlag==1:
    query_predict_train_high_pathfile=open(query_predict_train_high_path)
    query_predict_train_high_seg_pathfile=open(query_predict_train_high_seg_path,'w+')

    query_predict_train_low_pathfile=open(query_predict_train_low_path)
    query_predict_train_low_seg_pathfile=open(query_predict_train_low_seg_path,'w+')

    infiles=[query_predict_train_high_pathfile,query_predict_train_low_pathfile]
    outfiles=[query_predict_train_high_seg_pathfile,query_predict_train_low_seg_pathfile]
    print 'seg...'
    for i in range(2):
        curinfile=infiles[i]
        curoutfile=outfiles[i]
        if i==0:
            cur_query_dict=high_query_dict
        else:
            cur_query_dict=low_query_dict
        for line in curinfile.readlines():
            line=line.strip()
            fields=line.split('\t')
            query=fields[0]
            if query=='':
                continue
            query=query.lower()
            oneword=True
            q0=query[0]
            for i in range(1,len(query)):
                if query[i]!=q0:
                    oneword=False
                    break
            uquery=unicode(query, 'utf-8')
            query=cht_to_chs(uquery)
            quality=float(fields[1])
            cut_list=cut_query(query)
            res = ' '.join(cut_list)
            res=res.encode('utf-8')
            uids=''
            if len(fields)>=3:
                uids='\t'+fields[2]
            en_wc = 0
            ch_wc = 0
            for uch in uquery:
                if (uch >= u'a' and uch <= u'z') or (uch >= u'A' and uch <= u'Z'):
                    en_wc += 1
                elif uch >= u'\u4e00' and uch <= u'\u9fff':
                    ch_wc += 1
            if ch_wc <= 1 and en_wc <= 1:
                continue
            if ch_wc >= 1:
                query = query.replace(' ', '')
            # query=query.replace(' ','')
            curoutfile.write(query+'\t'+fields[1]+'\t'+res+uids+'\n')
            cur_query_dict[query]=(quality,res,uids)
        curinfile.close()
        curoutfile.close()
else:
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
query_sim_h_path= deltadatadir + '/query.sim.h'
query_sim_l_path= deltadatadir + '/query.sim.l'
query_sim_delta_path= deltadatadir + '/query.sim.delta'

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

#query语义相似
query_sim_hl_dict={}
process_w2v_simout(query_sim_h_path,query_sim_hl_dict,[high_query_dict])
process_w2v_simout(query_sim_l_path,query_sim_hl_dict,[low_query_dict],sim_thr=0.5)
process_w2v_simout(query_sim_delta_path, query_sim_hl_dict, [high_query_dict,low_query_dict])
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
query_ed_path= deltadatadir + '/query.editdist.all.delta'
process_ed_simout(query_ed_path,query_ed_hl_dict)
# query_ed_hh_path= deltadatadir + '/query.editdist.full.hh'
# query_ed_lh_path= deltadatadir + '/query.editdist.full.lh'
# process_ed_simout(query_ed_hh_path,query_ed_hl_dict)
# process_ed_simout(query_ed_lh_path,query_ed_hl_dict)
# for date in  date_arr:
#     date_query_edsim_path = deltadatadir + '/query.editdist.' + date
#     process_ed_simout(date_query_edsim_path, query_ed_hl_dict)
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
sequential_outsimpath=sequential_dir+'/full.query.sequential.view'
process_sequential_simout(sequential_outsimpath,query_sequential_hl_dict)
print 'query_sequential_hl_dict len=',len(query_sequential_hl_dict)

outhsimfile= open(deltadatadir + '/query.h.sim.out','w+')
s_outhsimfile= open(deltadatadir + '/query.h.sim.out.simple','w+')
outlsimfile= open(deltadatadir + '/query.l.sim.out','w+')
s_outlsimfile= open(deltadatadir + '/query.l.sim.out.simple','w+')
outsimfile_arr=[outhsimfile,s_outhsimfile,outlsimfile,s_outlsimfile]

query_predict_train_high_seg_pathfile=open(query_predict_train_high_path+'.seg')
query_predict_train_low_seg_pathfile=open(query_predict_train_low_path+'.seg')
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