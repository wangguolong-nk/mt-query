# -*- coding:utf-8 -*-
'''
mode=1:每天产生的query 2:全量的低质量到高质量 3:全量的高质量到高质量
mode=1时：
输入：query.20170520及query.predict.train.high.seg和query.editdist.all.delta
输出：query.editdist.20170520，query.editdist.all.delta(更新)
'''

import Levenshtein
import re
import time
import os

class QueryNode:
    def __init__(self):
        self.__id=-1
        self.__similarSet = set()
        self.__query = None
        self.__quality = 0.0
        self.__isOver=False
    def isOver(self):
        return self.__isOver
    def set_isOver(self,isOver):
        self.__isOver=isOver
    def get_id(self):
        return self.__id
    def set_id(self,id):
        self.__id=id
    def get_query(self):
        return self.__query
    def set_query(self,query):
        self.__query=query
        if len(self.__query)==1:
            self.set_isOver(True)
    def set_quality(self,quality):
        self.__quality=quality
    def get_quality(self):
        return self.__quality
    def addSimilarQuery(self,q):
        return self.__similarSet.add(q)
    def get_similarset(self):
        return self.__similarSet
class QueryEditDist:
    def __init__(self, outdir, delta_querydir, date, full_querydir,mode,deleteFlag):
        self.outdir = outdir
        self.hqpath = full_querydir + '/query.predict.train.high.seg'
        self.mode=mode#1:每天产生的query 2:全量的低质量到高质量 3:全量的高质量到高质量
        self.deleteFlag=deleteFlag
        self.deletepath=None
        self.query_editdist_all_delta_path=delta_querydir+'/query.editdist.all.delta'
        if mode==1:
            self.delta_query_filepath = delta_querydir + '/query.' + date
            self.deletepath=self.delta_query_filepath
            self.delta_editdist_outpath = delta_querydir + '/query.editdist.' + date
        elif mode==2:
            self.delta_query_filepath=full_querydir+'/query.predict.train.low.seg'
            self.delta_editdist_outpath = delta_querydir + '/query.editdist.full.lh'
        else:#mode=3
            self.delta_query_filepath=self.hqpath
            self.delta_editdist_outpath = delta_querydir + '/query.editdist.full.hh'

        print self.delta_query_filepath,'-+->',self.hqpath
        print '==>',self.delta_editdist_outpath
        self.pattern = re.compile(u'([\u4e00-\u9fa5]+)')
        self.high_code_node_dict={}
        self.low_code_node_dict={}
        self.high_nodes=[]
        self.low_nodes=[]
        self.loadData(False)
        self.loadData(True)

    def loadData(self,isdeltadate):
        print 'start loadData'
        loadData_t0=time.time()
        if not isdeltadate:
            code_node_dict=self.high_code_node_dict
            nodes=self.high_nodes
            query_file=open(self.hqpath)
        else:
            code_node_dict=self.low_code_node_dict
            nodes=self.low_nodes
            query_file=open(self.delta_query_filepath)
        wc=0
        for line in query_file.readlines():
            line=line.strip()
            quality=0.0
            fields = line.split('\t')
            query_word = fields[0].strip()
            if len(fields) == 2:
                quality = float(fields[1])
            query_word = unicode(query_word, 'utf-8')
            wc+=1
            qn=QueryNode()
            qn.set_id(wc)
            qn.set_query(query_word)
            qn.set_quality(quality)
            qn_word_len=len(query_word)
            for ch in query_word:
                code=ord(ch)*1000+qn_word_len
                if code in code_node_dict:
                    code_node_dict[code].append(qn)
                else:
                    code_node_dict[code]=[qn]
            nodes.append(qn)
        query_file.close()
        print 'end loadData,time=',(time.time()-loadData_t0),',query数量=',wc

    def compareAll(self):
        print 'start compareAll...'
        editdist_sim_out_file = open(self.delta_editdist_outpath, 'w+')
        self.compare_ts0=time.time()
        for query_node in self.low_nodes:
            self.compareOne(query_node,editdist_sim_out_file)
        editdist_sim_out_file.close()
        if self.deletepath:
            import os
            os.remove(self.deletepath)
    def compareOne(self,low_query_node,editdist_sim_out_file):
        low_query_word = low_query_node.get_query()
        low_query_word_len=len(low_query_word)
        query_node_id=low_query_node.get_id()
        # assert isinstance(low_query_node,QueryNode)
        msg=low_query_word+'('+str(low_query_node.get_quality())+')\t'
        msg_sim=''
        if query_node_id%2000==0:
            temp_t=time.time()
            print 'compare ',query_node_id,'\ttime=',(temp_t-self.compare_ts0)
            self.compare_ts0=temp_t
        idset=set()
        dist_wc=0
        for w in low_query_word:
            # assert isinstance(w,unicode)
            if low_query_word_len<=2:
                continue
            low_code=ord(w)*1000+low_query_word_len
            high_codes=[low_code-2,low_code-1,low_code,low_code+1,low_code+2]
            for high_code in high_codes:
                if high_code not in self.high_code_node_dict:
                    continue
                high_code_nodelist=self.high_code_node_dict[high_code]
                # assert isinstance(high_code_nodelist,list)
                for high_code_node in high_code_nodelist:
                    # assert isinstance(high_code_node,QueryNode)
                    high_query_word=high_code_node.get_query()
                    high_code_id=high_code_node.get_id()
                    if high_code_id in idset:
                        continue
                    else:
                        idset.add(high_code_id)
                    if low_query_word==high_query_word:
                        continue
                    temp_dist=Levenshtein.distance(low_query_word,high_query_word)
                    dist_wc+=1
                    cur_dist_thr = 2
                    if low_query_word_len <= 4:
                        cur_dist_thr = 1
                    if temp_dist<=cur_dist_thr:#114万秒
                        low_query_node.addSimilarQuery(high_code_node)
                        msg_sim+=high_query_word+'('+str(high_code_node.get_quality())+') '
        if msg_sim!='':
            msg=msg+msg_sim
            msg=msg.strip()
            msg = msg.encode('utf-8')
            editdist_sim_out_file.write(msg+'\n')
            if query_node_id%1000==0:
                editdist_sim_out_file.flush()
    def separate(self,source):#1:en,2:ch
        if not source:
            return ['', '']
        if isinstance(source, str):
            source = unicode(source, 'utf-8')
        source = source.strip()
        if source == '':
            return [u'', u'']
        results = self.pattern.split(source)
        str1 = u''
        str2 = u''
        for i in range(len(results)):
            if i % 2 == 0:
                str1 += results[i]
            else:
                str2 += results[i]
        if str1!=u'':
            if self.is_chinese(str1[0]):
                return [str2,str1]
            else:
                return [str1,str2]
        elif str1==u'' and str2!=u'':
            if self.is_chinese(str2[0]):
                return [str1,str2]
            else:
                return [str2,str1]
        else:#str1==u'' and str2==u''
            return [u'',u'']
    def is_chinese(self,uchar):
        if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
            return True
        else:
            return False
    def merge_delta(self):
        if self.mode!=1:
            return
        paths=[]
        paths.append(self.query_editdist_all_delta_path)
        paths.append(self.delta_editdist_outpath)
        query2ed_dict = {}
        for ed_path in paths:
            with open(ed_path) as fi:
                print ed_path
                for line in fi.readlines():
                    line = line.strip()
                    fields = line.split('\t')
                    query = fields[0]
                    query_fields = query.split('(')
                    query = query_fields[0]
                    if len(fields) < 2:
                        continue
                    query2ed_dict[query] = line
        outpath = self.query_editdist_all_delta_path+'.del'
        with open(outpath, 'w+') as fo:
            for query in query2ed_dict:
                edline = query2ed_dict[query]
                fo.write(edline + '\n')
        os.remove(self.query_editdist_all_delta_path)
        os.rename(outpath,self.query_editdist_all_delta_path)
        if self.deleteFlag==1:
            print 'delete:',self.delta_editdist_outpath
            os.remove(self.delta_editdist_outpath)
import sys
if __name__=='__main__':
    date=None
    projdatadir=None
    mode=1#1:每天产生的query 2:全量的低质量到高质量 3:全量的高质量到高质量
    deleteFlag=0
    for i in range(1,len(sys.argv)):
        paramkv = sys.argv[i].split('=')
        paramk = paramkv[0]
        val=paramkv[1]
        if paramk=='-projdatadir':
            projdatadir=val
        elif paramk=='-date':
            date=val
        elif paramk=='-mode':
            mode=int(val)
        elif paramk=='-delete':
            deleteFlag=int(val)
    if mode!=1 and mode!=2 and mode!=3:
        print 'ERROR:mode!=1 and mode!=2 and mode!=3'
        sys.exit(2)
    if mode==1:
        if not date:
            print 'mode=1 MUST NEED parameter: date'
            sys.exit(2)
    if not projdatadir:
        print 'need PARAMETER: projdatadir!'
        sys.exit(2)
    print 'mode=',mode
    delta_querydir = projdatadir+'/work_data/search/delta_data'
    full_querydir = projdatadir+'/work_data/search'
    outdir = delta_querydir
    qdist=QueryEditDist(outdir=outdir,delta_querydir=delta_querydir,full_querydir=full_querydir,date=date,mode=mode,deleteFlag=deleteFlag)
    compareAll_t0=time.time()
    qdist.compareAll()
    print 'compareAll time=',(time.time()-compareAll_t0)
    qdist.merge_delta()
    print 'over'