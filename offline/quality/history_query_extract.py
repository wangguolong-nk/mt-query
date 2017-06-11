# -*- coding:utf-8 -*-
import sys
import re

datadir=None
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    print argk,',',argv
    if argk=='-datadir':
        datadir=argv
if not datadir:
    print 'MUST NEED parameter: datadir!'
    sys.exit(2)
path=datadir+'/search_word_count.txt'
file=open(path)
wc=0
num=0
lnum=0
hpath=datadir+'/search_word_counter.wc.high'
lpath=datadir+'/search_word_counter.wc.low'

hfile=open(hpath,'w+')
lfile=open(lpath,'w+')
# pattern=u'[^a-zA-Z\u4e00-\u9fff]+'
for line in file.readlines():
    line=line.strip()
    fields=line.split('\t')
    wc+=1
    if wc%1000000==0:
        print wc
    query=fields[1]
    query=query.strip()
    uquery=unicode(query,'utf-8')
    query_len = len(query)
    if query_len == 0:
        # print 'come4'
        continue

    # uquery = re.sub(pattern, ' ', uquery)
    en_wc = 0  # 字母个数
    ch_wc = 0  # 汉字个数
    isokstr=True
    for uch in uquery:
        if (uch >= u'a' and uch <= u'z') or (uch >= u'A' and uch <= u'Z'):
            en_wc += 1
        elif uch >= u'\u4e00' and uch <= u'\u9fff':
            ch_wc += 1
        else:
            isokstr=False
            break
    if not isokstr:
        continue
    # print 'en_wc',en_wc,'ch_wc',ch_wc,'line=',line
    if en_wc!=0 and ch_wc!=0:
        # print 'come1'
        continue
    if en_wc+ch_wc<=1:
        continue
    if en_wc<=3 and en_wc>0:
        # print 'come2'
        continue
    if ch_wc<2 and ch_wc>0:
        # print 'come3'
        continue
    query=uquery.encode('utf-8')
    # query=query.strip()

    try:
        counter = int(fields[2])
    except Exception,e:
        try:
            counter=int(fields[len(fields)-1])
        except Exception,e2:
            print line
            print 'Exception...'
            sys.exit(1)
    newline=fields[0]+'\t'+query+'\t'+fields[2]
    if counter>50:
        num+=1
        hfile.write(line+'\n')
    else:
        lfile.write(line+'\n')
hfile.close()
file.close()
lfile.close()
print 'num=',num
print 'hnum=',num
print 'lnum=',lnum