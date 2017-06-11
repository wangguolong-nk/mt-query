# -*- coding:utf-8 -*-
import sys
import re

raw_path='/data1/wgl/meipai-search-suggest-offline-alg-data/quality/work_data/query.nick'
out_path=raw_path+'.new'
out_path2=raw_path+'.new2'
raw_f=open(raw_path)
outf=open(out_path,'w+')
outf2=open(out_path2,'w+')
pattern = u'[^a-zA-Z\u4e00-\u9fff]+'
for line in raw_f.readlines():
    fields=line.strip().split('\t')
    if len(fields)<2:
        continue
    query=fields[0]
    query_word = unicode(query, 'utf-8')
    query_word = re.sub(pattern, ' ', query_word)
    query_word = query_word.strip()
    if len(query_word) == 1:
        continue
    query_word = query_word.encode('utf-8')
    if query_word == '':
        continue
    outf.write(query_word+'\t'+query+'\t'+fields[1]+'\n')
    outf2.write(query_word+'\t'+fields[1]+'\n')
outf.close()
outf2.close()
raw_f.close()