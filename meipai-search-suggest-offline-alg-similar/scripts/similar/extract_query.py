# -*- coding:utf-8 -*-
'''
整理出该天的query
输入：search.user_mv.20170520
输出：query.20170520
'''
import os
import re
class Merge():
    def __init__(self,date=None,data_dir=None,deleteFlag=0):
        if data_dir:
            self.data_dir=data_dir
        if not date:
            assert 1==0
        self.search_path=self.data_dir+'/search.user_mv.'+str(date)
        self.search_query_sort_path=self.data_dir+'/query.'+str(date)
        self.separator=chr(2)
        self.deleteFlag=deleteFlag
    def work(self):
        pattern = u'[^a-zA-Z\u4e00-\u9fff]+'
        out_file = open(self.search_query_sort_path, 'w+')
        search_query_file = open(self.search_path)
        query_set=set()
        for line in search_query_file.readlines():
            fields = line.split('\t')
            query_word = fields[5].strip()
            query_word = unicode(query_word, 'utf-8')
            query_word = re.sub(pattern, ' ', query_word)
            query_word = query_word.strip()
            if len(query_word)==1:
                continue
            query_word = query_word.encode('utf-8')
            if query_word=='':
                continue
            query_set.add(query_word)
        temp_wc = 0
        for query_word in query_set:
            temp_wc += 1
            out_file.write(query_word + '\n')
        out_file.close()
        if self.deleteFlag==1:
            os.remove(self.search_path)

import sys
if __name__=='__main__':
    data_dir = None
    dates=None
    deleteFlag=0
    for i in range(1,len(sys.argv)):
        paramkv=sys.argv[i].split('=')
        paramk=paramkv[0]
        val=paramkv[1]
        if paramk=='-dates':
            dates=val
        elif paramk=='-data_dir':
            data_dir=val
        elif paramk=='-delete':
            deleteFlag=int(val)
    if not dates or not data_dir:
        print 'MUST NEED parameter :dates and data_dir!'
        sys.exit(1)
    date_arr = dates.split(',')
    if len(date_arr) == 1:
        date_arr = dates.split('-')
        if len(date_arr) == 1:
            date_arr = [dates,dates]
    date_arr=range(int(date_arr[0]),int(date_arr[len(date_arr)-1])+1)
    print date_arr
    for date_item in date_arr:
        merge=Merge(date=date_item,data_dir=data_dir,deleteFlag=deleteFlag)
        merge.work()