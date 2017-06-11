# -*- coding:utf-8 -*-
import os
import re
class Merge():
    def __init__(self,date=None,data_dir=None,gray_val=None):
        if data_dir:
            self.data_dir=data_dir
        if not date:
            assert 1==0
        self.search_path=self.data_dir+'/search.user_mv.'+str(date)
        self.search_query_sort_path=self.data_dir+'/query.'+str(date)#+'/query.'+date
        self.mvid_path=self.data_dir+'/mvid.'+str(date)
        print 'search_path=',self.search_path
        print 'search_query_sort_path=',self.search_query_sort_path
        self.separator=chr(2)
        if gray_val:
            self.gray_val=map(int,gray_val.split(','))
        else:
            self.gray_val=None
    def work(self):
        pattern = u'[^a-zA-Z\u4e00-\u9fff]+'
        search_dict = {}
        out_file = open(self.search_query_sort_path, 'w+')
        mvid_pathfile=open(self.mvid_path,'w+')
        result_file = open(self.search_path)
        mvid_set=set()
        for line in result_file.readlines():
            fields = line.split('\t')
            uid=fields[0]
            if self.gray_val:
                uid_end_num=int(uid[-1])
                if uid_end_num not in self.gray_val:
                    continue
            query_word = fields[5].strip()
            query_word = unicode(query_word, 'utf-8')
            query_word = re.sub(pattern, ' ', query_word)
            query_word = query_word.strip()
            if len(query_word)==1:
                continue
            query_word = query_word.encode('utf-8')
            if query_word=='':
                continue
            mv_result=fields[3]
            page=fields[1]
            page=int(page)
            if page!=1:
                continue
            if mv_result=='\\N':
                continue
            mv_result_arr=mv_result.split(self.separator)
            for mvid in mv_result_arr:
                mvid_set.add(mvid)
            new_mv_result=','.join(mv_result_arr)
            newline=query_word+'\t'+new_mv_result
            search_dict[query_word] = newline
        temp_wc = 0
        for query_word in search_dict:  # 相同的query放在上下相邻行
            temp_wc += 1
            out_file.write(search_dict[query_word] + '\n')
            if temp_wc<=4:
                print search_dict[query_word] + '\n'
            if temp_wc % 10000 == 0:
                out_file.flush()
        out_file.close()
        search_dict.clear()
        wc=0
        for mvid in mvid_set:
            wc+=1
            mvid_pathfile.write(mvid + '\n')
            if wc % 50000 == 0:
                mvid_pathfile.flush()
        mvid_pathfile.close()
        if deleteFlag==1:
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
        merge=Merge(date=date_item,data_dir=search_data_dir,gray_val=gray_val)
        merge.work()