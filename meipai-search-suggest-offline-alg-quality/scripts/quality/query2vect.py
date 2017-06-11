# -*- coding:utf-8 -*-
#query详情转向量
import sys
import os

startdate=None
enddate=None
dates=None
datadir=None
history_search_path=None
deleteFlag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    print argk,',',argv
    if argk=='-datadir':
        datadir=argv
    elif argk=='-startdate':
        startdate=int(argv)
    elif argk=='-enddate':
        enddate=int(argv)
    elif argk == '-dates':
        dates = argv
    elif argk=='-history_search_path':
        history_search_path=argv
    elif argk=='-delete':
        deleteFlag=int(argv)
if not datadir or not history_search_path:
    print 'MUST NEED parameter: datadir and history_search_path!'
    sys.exit(1)
if not startdate or not enddate:
    if not dates:
        sys.exit(1)
    date_arr=dates.split(',')
else:
    date_arr=range(startdate, enddate + 1)
print 'date_arr=',date_arr
print 'history_search_path=',history_search_path
history_search_file=open(history_search_path)
history_dict={}
print 'start load history query data...'
error_wc=0
wc=0
for line in history_search_file.readlines():
    fields=line.strip().split('\t')
    wc+=1
    if wc%5000000==0:#50700000
        print wc
    if len(fields)!=3:
        error_wc+=1
        continue
    old_query=fields[1]
    old_query=unicode(old_query,'utf-8')
    old_query_wc=fields[2]
    # print old_query,old_query_wc
    history_dict[old_query]=old_query_wc
history_search_file.close()
print 'error_wc=',error_wc
for date in date_arr:
    fromdatapath=datadir+"/query."+str(date)+".detail"
    tooutpath=datadir+"/query."+str(date)+".vect"
    fromdatapathfile=open(fromdatapath)
    tooutpathfile=open(tooutpath,'w+')
    query_index_file=open(datadir+'/query.'+str(date)+".index",'w+')
    wc=0
    print tooutpath
    for line in fromdatapathfile.readlines():
        wc+=1
        if wc%50000==0:
            print wc
        # if wc>10000:
        #     break
        fields=line.strip().split('\t')
        query=fields[0]
        uquery=unicode(query,'utf-8')
        en_wc=0#字母个数
        ch_wc=0#汉字个数
        for uch in uquery:
            if (uch >= u'a' and uch <= u'z') or (uch >= u'A' and uch <= u'Z'):
                en_wc += 1
            elif uch >= u'\u4e00' and uch <= u'\u9fff':
                ch_wc += 1
        query_history_wc=0
        if uquery in history_dict:
            query_history_wc=history_dict[uquery]
        mvdetaillist=fields[1]
        mvNicknames=fields[2]
        seg0str=fields[3]
        plays_count_sum=0#视频播放数总和(平均)
        reposts_count_sum=0#视频转发数总和(平均)
        # brush_likes_count_sum=0#视频刷赞数总和(平均)
        share_count_sum=0#视频分享数总和(平均)
        comments_count_sum=0#视频评论数总和(平均)
        likes_count_sum=0#视频喜欢数总和(平均)
        # isrecent=1#是否是7天内的数据
        mvdetaillist_arr=mvdetaillist.split(',')
        # mv_counter=len(mvdetaillist_arr)#query搜索结果个数
        # mv_counter_f=float(mv_counter)
        mv_counter=0#有效结果个数
        match_mv_uname_counter=0#query匹配视频主人昵称的个数
        match_full_mv_text_counter=0#query匹配视频短文本(全部：query切词都出现在该视频的短文本)的个数
        match_part_mv_text_counter=0#query匹配视频短文本(部分)的个数
        isusernickname=0#query本身是否昵称类query
        isusernickname_wc=0
        text_match_wc=0
        user_followers_counter=0
        user_videos_count=0
        user_friends_count=0
        for i in range(len(mvdetaillist_arr)):
            if i>=20:
                break
            mvdetail=mvdetaillist_arr[i]
            mvdetail_fields=mvdetail.split('|')
            mv_text_code = int(mvdetail_fields[14])
            cur_isusernickname=int(mvdetail_fields[15])
            if mv_text_code != 2 and cur_isusernickname!=1:
                continue
            mv_counter+=1
            if cur_isusernickname==1:
                if isusernickname==0:
                    isusernickname=cur_isusernickname
                    user_followers_counter=int(mvdetail_fields[11])
                    user_videos_count=int(mvdetail_fields[12])
                    user_friends_count=int(mvdetail_fields[13])
                isusernickname_wc+=1
            if mv_text_code==2:
                text_match_wc+=1
            plays_count_sum+=int(mvdetail_fields[1])
            reposts_count_sum+=int(mvdetail_fields[2])
            share_count_sum+=int(mvdetail_fields[4])
            comments_count_sum+=int(mvdetail_fields[5])
            likes_count_sum+=int(mvdetail_fields[6])
        if mv_counter==0:
            plays_count_sum = 0
            reposts_count_sum = 0
            share_count_sum = 0
            comments_count_sum = 0
            likes_count_sum = 0
        else:
            mv_counterf=float(mv_counter)
            plays_count_sum=plays_count_sum/mv_counterf
            reposts_count_sum=reposts_count_sum/mv_counterf
            share_count_sum=share_count_sum/mv_counterf
            comments_count_sum=comments_count_sum/mv_counterf
            likes_count_sum=likes_count_sum/mv_counterf
        one_vect = str(wc) + ',' + str(query_history_wc) + ',' + str(plays_count_sum) + ',' + \
                   str(reposts_count_sum+share_count_sum) + ',' + str(comments_count_sum) + ',' + \
                   str(likes_count_sum) + ',' + str(mv_counter) + ',' + str(ch_wc) + ',' + str(en_wc) + \
                   ',' + str(isusernickname_wc)+','+str(text_match_wc)+','+str(user_followers_counter)+','+\
                   str(user_videos_count)+','+str(user_friends_count)#14个  特征：[1,6]
        #19个特征(除了id)
        query_index_file.write(str(wc)+'\t'+query+'\t'+mvNicknames+'\t'+seg0str+'\n')
        tooutpathfile.write(one_vect+'\n')
        if wc%50000==0:
            query_index_file.flush()
            tooutpathfile.flush()
    query_index_file.close()
    fromdatapathfile.close()
    tooutpathfile.close()
    if deleteFlag==1:
        os.remove(fromdatapath)