# -*- coding:utf-8 -*-
import os
import re
class Merge():
    def __init__(self,date=None,data_dir=None,gray_val=None,deleteFlag=0):
        if data_dir:
            self.data_dir=data_dir
        if not date:
            assert 1==0
        self.search_path=self.data_dir+'/search.user_mv.'+str(date)
        self.search_query_sort_path=self.data_dir+'/query.seq.'+str(date)
        print 'search_path=',self.search_path
        print 'search_query_sort_path=',self.search_query_sort_path
        self.separator=chr(2)
        if gray_val:
            self.gray_val=map(int,gray_val.split(','))
        else:
            self.gray_val=None
        self.deleteFlag=deleteFlag
    def work(self):
        pattern = u'[^a-zA-Z\u4e00-\u9fff]+'
        search_dict = {}
        out_file = open(self.search_query_sort_path, 'w+')
        result_file = open(self.search_path)
        for line in result_file.readlines():
            fields = line.strip().split('\t')
            uid=fields[0]
            search_time=fields[2]
            if self.gray_val:
                uid_end_num=int(uid[-1])
                if uid_end_num not in self.gray_val:
                    continue
            query_word = fields[5].strip()
            query_word = unicode(query_word, 'utf-8')
            query_word = re.sub(pattern, ' ', query_word)
            query_word = query_word.strip()
            if len(query_word)<=1:
                continue
            query_word = query_word.encode('utf-8')
            mv_result=fields[3]
            if mv_result=='\\N':
                continue
            mv_result_arr=mv_result.split(self.separator)
            if len(mv_result_arr)<=1:
                continue
            newline=(query_word,search_time)
            search_dict.setdefault(uid,[])
            search_dict[uid].append(newline)
        for uid in search_dict:  # 相同的query放在上下相邻行
            uid_arr_val=search_dict[uid]
            dict_t={}
            for uid_item_val in uid_arr_val:
                dict_t[uid_item_val[0]]=uid_item_val[1]
            uid_arr_sortlist_val=sorted(dict_t.items(),key=lambda d:d[1],reverse=False)
            for uid_item_val in uid_arr_sortlist_val:
                query=uid_item_val[0]
                out_file.write(uid+'\t'+query+'\t'+uid_item_val[1]+'\n')
        out_file.close()
        search_dict.clear()
        if self.deleteFlag==1:
            os.remove(self.search_path)

import sys
if __name__=='__main__':
    search_data_dir = None
    dates=None
    gray_val=None
    deleteFlag=0
    for i in range(1,len(sys.argv)):
        paramkv=sys.argv[i].split('=')
        paramk=paramkv[0]
        val=paramkv[1]
        if paramk=='-dates':
            dates=val
        elif paramk=='-search_data_dir':
            search_data_dir=val
        elif paramk=='-gray_val':
            gray_val=val
        elif paramk=='-delete':
            deleteFlag=int(val)
    if not dates or not search_data_dir:
        print 'MUST NEED parameter :dates and search_data_dir!'
        sys.exit(1)
    date_arr = dates.split(',')
    if len(date_arr) == 1:
        date_arr = dates.split('-')
        if len(date_arr) == 1:
            date_arr = [dates,dates]
    date_arr=range(int(date_arr[0]),int(date_arr[len(date_arr)-1])+1)
    print date_arr
    for date_item in date_arr:
        merge=Merge(date=date_item,data_dir=search_data_dir,gray_val=gray_val,deleteFlag=deleteFlag)
        merge.work()