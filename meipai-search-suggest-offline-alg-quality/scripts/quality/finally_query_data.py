# -*- coding:utf-8 -*-
import sys
import jieba
import re
import time

startdate=None
enddate=None
dates=None
deleteFlag=0
datadir=None

pattern=u'[^a-zA-Z\u4e00-\u9fff]+'
def seg(text,isstr=True):
    if isstr:
        text=unicode(text,'utf-8')
    text=re.sub(pattern,' ',text)
    text=text.strip()
    if text=='':
        return set([])
    seg_list=jieba.cut(text)
    seg_set=set([v.strip() for v in seg_list if v.strip()!=''])
    return seg_set
def compare(query_set1,seg_set2):
    match_wc = 0
    for q in query_set1:
        if q in seg_set2:
            match_wc += 1
    if match_wc == 0:
        return 0
    elif match_wc == len(query_set1):
        return 2
    elif match_wc < len(query_set1):
        return 1
    else:
        return 0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    print argk,',',argv,type(argv)
    if argk=='-datadir':
        datadir=argv
    elif argk=='-startdate':
        startdate=int(argv)
    elif argk=='-enddate':
        enddate=int(argv)
    elif argk=='-dates':
        dates=argv
    elif argk=='-delete':
        deleteFlag=int(argv)
if not datadir:
    print 'MUST NEED parameter: datadir!'
if not startdate or not enddate:
    if not dates:
        sys.exit(1)
    date_arr=dates.split(',')
else:
    date_arr=range(startdate, enddate + 1)
punct = set(u'''
:!),.:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～
￠々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻︽︿﹁﹃﹙﹛﹝（｛“‘-—_…#
''')
print 'date_arr=',date_arr
for date in date_arr:
  queryrawpath=datadir+'/query.'+str(date)
  queryrawfile=open(queryrawpath)
  mvdetailpath=datadir+'/mvid.'+str(date)+'.detail'
  print mvdetailpath
  mvdetailfile=open(mvdetailpath)
  mvid_detail_dict={}
  wc=0
  preline=''
  for line in mvdetailfile.readlines():
    wc+=1
    fields=line.strip().split('\t')
    if len(fields)<2:
        # print wc,line
        # sys.exit(1)
        preline=preline+line
        continue
    if preline!='':
        prefields = preline.strip().split('\t')
        mvdetaillist = prefields[1]
        mvid = prefields[0]
        mvid_detail_dict[mvid] = mvdetaillist
        preline=''
    mvdetaillist=fields[1]
    mvid=fields[0]
    mvid_detail_dict[mvid]=mvdetaillist
    preline=line

  mvdetailfile.close()
  print 'read over ',mvdetailpath
  outfile=open(queryrawpath+'.detail','w+')
  wc=0
  print 'start ',queryrawpath,'... 生成详情...'
  continue_wc=0
  t0=time.time()
  for line in queryrawfile.readlines():
    wc+=1
    # if wc>10000:
    #     break
    fields=line.strip().split('\t')
    query=fields[0]
    queryisNickname=False
    queryNickname_set=set()
    query = query.lower()
    uquery=unicode(query,'utf-8')
    query_set=seg(uquery,False)
    if len(fields)<2:
        # print line
        continue
    en_wc = 0
    ch_wc = 0
    for uch in uquery:
        if (uch >= u'a' and uch <= u'z') or (uch >= u'A' and uch <= u'Z'):
            en_wc += 1
        elif uch >= u'\u4e00' and uch <= u'\u9fff':
            ch_wc += 1
    if ch_wc<=1 and en_wc<=1:
        continue
    if ch_wc>=1:
        query=query.replace(' ','')
    mvlist=fields[1]
    # newline=fields[0]+'\t'+fields[1]+'\t'
    newline=query+'\t'
    mvlist_arr=mvlist.split(',')
    flag=False
    for mvid in mvlist_arr:
        if mvid in mvid_detail_dict:
            flag=True
            mvid_detail_val=mvid_detail_dict[mvid]
            mvid_detail_val_fields=mvid_detail_val.split("|")
            mvid_detail_before=mvid_detail_val_fields[0]+"|"+mvid_detail_val_fields[1]+"|"+mvid_detail_val_fields[2]\
                               +"|"+mvid_detail_val_fields[3]+"|"+mvid_detail_val_fields[4]+"|"+mvid_detail_val_fields[5]\
                               +"|"+mvid_detail_val_fields[6]+"|"+mvid_detail_val_fields[7]+"|"+mvid_detail_val_fields[8] \
                               + "|" + mvid_detail_val_fields[9]+"|"+mvid_detail_val_fields[10]+"|"+\
                               mvid_detail_val_fields[11]+"|"+mvid_detail_val_fields[12]
            thismsg=mvid_detail_before
            mvid_ownerid=mvid_detail_val_fields[6]
            mvid_detail_u_nickname_raw=mvid_detail_val_fields[13]#馨花漾®其臻
            mvid_detail_u_nickname = mvid_detail_u_nickname_raw.lower()
            # mvid_detail_u_nickname = '馨花漾®其臻.>'
            mvid_detail_u_nickname_seg = seg(mvid_detail_u_nickname)
            nicknameflag = False
            if len(query_set) == len(mvid_detail_u_nickname_seg) and len(query_set)>0:
                nicknameflag=True
                for s in query_set:
                    if s not in mvid_detail_u_nickname_seg:
                        nicknameflag = False
                if nicknameflag:
                    queryisNickname=True
                    queryNickname_set.add(mvid_ownerid)
            start_index=len(mvid_detail_before)+1+len(mvid_detail_u_nickname)+1
            mvid_detail_text=mvid_detail_val[start_index:]#视频短文本
            mvid_detail_text = mvid_detail_text.lower()
            mvid_detail_text = re.sub('@\S+', ' ', mvid_detail_text)
            mvid_detail_text = re.sub('http://[a-zA-Z0-9.?/&=:]*', '', mvid_detail_text)
            mv_text_set=seg(mvid_detail_text)
            mv_text_code=compare(query_set,mv_text_set)
            thismsg += "|"+str(mv_text_code)
            if nicknameflag:
                thismsg+="|1"
            else:
                thismsg+="|0"
            newline+=mvid+'|'+thismsg+','
    if not flag:
        continue_wc+=1
        continue
    newline=newline[:-1]
    if queryisNickname:
        newline=newline+'\t'+'|'.join(queryNickname_set)
    else:
        newline=newline+'\t0'
    seg0str=' '.join(query_set)
    newline+='\t'+seg0str.encode('utf-8')
    outfile.write(newline+'\n')
    if wc%100000==0:
        print wc
        outfile.flush()
  outfile.close()
  queryrawfile.close()
  print 'continue_wc=',continue_wc
  t1=time.time()
  print 'time=',t1-t0
  if deleteFlag==1:
    import os
    mvpath = datadir + '/mvid.' + str(date)
    os.remove(mvdetailpath)
    os.remove(mvpath)