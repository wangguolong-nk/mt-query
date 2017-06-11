# -*- coding:utf-8 -*-
import os
import threading
import time
class EsData(threading.Thread):
    overFlag=False
    def __init__(self,query_set,query_path_part,mvid_set,part_index):
        threading.Thread.__init__(self)
        self.query_set=query_set
        self.query_path_part=query_path_part
        self.mvid_set=mvid_set
        self.part_index=part_index
    def run(self):
        query_file=open(self.query_path_part,'w+')
        wc=0
        pre_time=time.time()
        for query in self.query_set:
            query=query.strip()
            wc+=1
            query_es = re.split('\s+', query)
            query_es=','.join(query_es)
            if wc%5000==0:
                print
                query_file.flush()
                curtime=time.time()
                delta_time=curtime-pre_time
                print self.part_index,'=',wc,'time=',delta_time,'avg_time=',delta_time/5000.0
                pre_time=curtime
            mvlist=self.es_data(query_es)
            for mvid in mvlist:
                self.mvid_set.add(mvid)
            mvliststr = ','.join(str(s) for s in mvlist)
            query_file.write(query+'\t'+mvliststr+'\n')
        query_file.close()
        self.overFlag=True
        print 'self.mvid_set len=',len(self.mvid_set)
    def es_data(self,query):
            url = 'http://192.168.134.154:8090/media-search-api/query?query='+query+'&uid=1&from=0&size=20'
            req = urllib2.Request(url)
            req_res = urllib2.urlopen(req)
            res = req_res.read()
            res = json.loads(res)
            mvlist = res["result"]
            return mvlist
    def isOver(self):
        return self.overFlag

class Merge():
    def __init__(self,history_query_path=None,data_dir=None):
        if data_dir:
            self.data_dir=data_dir
        if not history_query_path:
            assert 1==0
        self.history_query_path=history_query_path
        print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.separator=chr(2)


    def work(self):
        query_set = set()
        query_path = self.data_dir + '/query.20501219'
        history_query_pathfile = open(self.history_query_path)
        print 'self.history_query_path=',self.history_query_path
        wc=0
        for line in history_query_pathfile.readlines():
            wc+=1
            query_set.add(line.strip())
        print 'query_set len=',len(query_set)
        history_query_pathfile.close()
        len1 = len(query_set)
        print 'len1=', len1
        set1=set()
        set2=set()
        wc=0
        for query in query_set:
            wc+=1
            if wc<len1/2:
                set1.add(query)
            else:
                set2.add(query)
        print 'set1 len=',len(set1)
        print 'set2 len=',len(set2)
        mvid_set1=set()
        mvid_set2=set()
        part1=query_path+'.part1'
        part2=query_path+'.part2'
        esdata1=EsData(set1,part1,mvid_set1,1)
        esdata2=EsData(set2,part2,mvid_set2,2)
        esdata1.start()
        esdata2.start()
        while not esdata1.isOver() or not esdata2.isOver():
            pass
        print 'esdata1.isOver()=',esdata1.isOver()
        print 'esdata2.isOver()=',esdata2.isOver()
        dict1_file=open(part1)
        print 'mvid_set1=',len(mvid_set1)
        print 'mvid_set2=',len(mvid_set2)
        dict2_file=open(part2)
        query_file = open(query_path, 'w+')
        for query_line in dict1_file.readlines():
            query_file.write(query_line)
        dict1_file.close()
        for query_line in dict2_file.readlines():
            query_file.write(query_line)
        dict2_file.close()
        query_file.close()
        for mvid in mvid_set2:
            mvid_set1.add(mvid)
        mvid_path = self.data_dir + '/mvid.20501219'
        query_mvid_file = open(mvid_path, 'w+')
        for mvid in mvid_set1:
            query_mvid_file.write(str(mvid)+'\n')
        query_mvid_file.close()
        os.remove(part1)
        os.remove(part2)
        print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

import urllib2
import re
import json
import datetime
if __name__=='__main__':
    url = 'http://192.168.134.154:8090/media-search-api/query?query=旭梅&uid=1&from=0&size=20'
    req = urllib2.Request(url)
    req_res = urllib2.urlopen(req)
    res = req_res.read()
    print 'res=', res
    datadir='/data1/wgl/deploy_data/raw_data'
    history_query_path = datadir+'/search_word_counter.wc.low.query'
    merge = Merge(history_query_path=history_query_path, data_dir=datadir)
    merge.work()