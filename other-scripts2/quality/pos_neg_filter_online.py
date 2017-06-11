# -*- coding:utf-8 -*-
import sys
import os

date_arg=None
mode=1#按小时#mode=2:按天
datadir=None
outdir=None
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    if argk=='-dates':
        date_arg=argv
    elif argk=='-mode':
        mode=int(argv)
    elif argk=='-datadir':
        datadir=argv
    elif argk=='-outdir':
        outdir=argv
if not date_arg or not datadir or not outdir:
    print 'need PARAMETER: -dates -datadir -outdir !'
    sys.exit(1)
if mode==1:
    date=int(date_arg)
    date_arr=range(date*100,date*100+23+1)
    new_date_arr=[]
    for date in date_arr:
        vectpath = datadir + '/query.' + str(date) + '.vect'
        if not os.path.exists(vectpath):
            continue
        new_date_arr.append(str(date))
    date_arr=new_date_arr
elif mode==2:
    date_arr=date_arg.split(',')
    if len(date_arr)==1:
        date_arr=date_arg.split('-')
        if len(date_arr)==2:
            new_date_arr=[]
            for i in range(int(date_arr[0]),int(date_arr[1])+1):
                new_date_arr.append(str(i))
            date_arr=new_date_arr
else:
    sys.exit(2)
print 'date_arr=',date_arr
for date in date_arr:
  vectpath=datadir+'/query.'+date+'.vect'
  vectfile=open(vectpath)
  indexpath=datadir + '/query.' + date + '.index'
  indexfile=open(indexpath)
  high_file=open(datadir+'/query.'+date+'.vect.high.delta','w+')
  low_file=open(datadir+'/query.'+date+'.vect.low.delta','w+')
  indexdict={}
  for line in indexfile.readlines():
        fields=line.strip().split('\t')
        index=fields[0]
        oldquery=fields[1]
        indexdict[index]=oldquery
  indexfile.close()
  hwc=0
  lwc=0
  for line in vectfile.readlines():
    line=line.strip()
    vect_fields=line.split(',')
    index=vect_fields[0]
    query=indexdict[index]
    if len(query)<=3:
        continue
    query_id=int(vect_fields[0])
    query_history_wc=float(vect_fields[1])
    plays_count_sum=float(vect_fields[2])
    reposts_share_count_sum=float(vect_fields[3])
    comments_count_sum=float(vect_fields[4])
    likes_count_sum=float(vect_fields[5])
    mv_counter=int(vect_fields[6])
    uquery_ch_len=int(vect_fields[7])
    uquery_en_len=int(vect_fields[8])
    isusernickname_wc=int(vect_fields[9])
    text_match_wc=int(vect_fields[10])

    comments_count_avg=comments_count_sum
    likes_count_avg=likes_count_sum
    plays_count_avg=plays_count_sum

    if (mv_counter<=1 and plays_count_avg<20) or (likes_count_avg<10 and comments_count_avg<5
                         and reposts_share_count_sum<=1 and mv_counter<3 and plays_count_avg<20):
        low_file.write(line + ',' + query + '\n')
        lwc += 1
        if lwc % 10000 == 0:
            print 'lwc=', lwc
        continue

    if (uquery_ch_len >= 3 or uquery_en_len >= 4) and mv_counter >=10 \
            and likes_count_avg > 1200 and comments_count_avg>700 and reposts_share_count_sum>=500 \
            and query_history_wc>=20:
        high_file.write(line+','+query+'\n')
        hwc += 1
        if hwc % 1000 == 0:
            print 'hwc=', hwc
  vectfile.close()
  low_file.close()
  high_file.close()
if 1==1:
    high_query_dict={}
    low_query_dict={}
    for date in date_arr:
        date=str(date)
        high_filepath=datadir+'/query.'+date+'.vect.high.delta'
        low_filepath=datadir+'/query.'+date+'.vect.low.delta'
        high_file=open(high_filepath)
        low_file=open(low_filepath)
        for line in high_file.readlines():
            fields=line.strip().split(',')
            query=fields[-1]
            high_query_dict[query]=line
        high_file.close()
        for line in low_file.readlines():
            fields = line.strip().split(',')
            query = fields[-1]
            low_query_dict[query] = line
        low_file.close()
        os.remove(high_filepath)
        os.remove(low_filepath)
    if len(date_arr)==1:
        filenameId=date_arr[0]
    else:
        filenameId=date_arr[0]+'-'+date_arr[-1]
        if mode==1:
            filenameId=date_arr[0]
            filenameId=filenameId[:-2]
    out_high_file=open(outdir+'/query.'+filenameId+'.vect.high.train','w+')
    print outdir+'/query.'+filenameId+'.vect.high.train'
    out_low_file=open(outdir+'/query.'+filenameId+'.vect.low.train','w+')
    for query in high_query_dict:
        line=high_query_dict[query]
        out_high_file.write(line)
    out_high_file.close()
    for query in low_query_dict:
        line=low_query_dict[query]
        out_low_file.write(line)
    out_low_file.close()