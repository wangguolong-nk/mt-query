# -*- coding:utf-8
import os
import sys
#我爱你
exit(2)
dates=None
modelmode='lr'
high_low_out_datadir=None
hourdatadir=None
deleteFlag=0
quality_predict_querydir=None
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    argk=fields[0]
    argv=fields[1]
    if argk=='-dates':
        dates=argv
    elif argk=='-modelmode':
        modelmode=argv
    elif argk=='-hourdatadir':
        hourdatadir=argv
    elif argk=='-high_low_out_datadir':
        high_low_out_datadir=argv
    elif argk=='-delete':
        deleteFlag=int(argv)
    elif argk=='-quality_predict_querydir':
        quality_predict_querydir=argv
if not high_low_out_datadir or not hourdatadir or not quality_predict_querydir:
    print 'MUST NEED parameter: high_low_out_datadir and hourdatadir and quality_predict_querydir!'
    sys.exit(2)
if not dates:
    print 'need PARAMETER: dates!'
    sys.exit(2)
if modelmode!='lr' and modelmode!='gbdt':
    sys.exit(2)
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr
print 'date_arr',date_arr
delta_high_dict={}#cur hour 高#当天的h&l
delta_low_dict={}#cur hour 低#当天的h&l
delta_file_line_wc=0
if modelmode == 'lr':
    high_query_path = quality_predict_querydir + '/query.quality.predict.high'
    low_query_path = quality_predict_querydir + '/query.quality.predict.low'
else:
    high_query_path = high_low_out_datadir + '/query.predict.gbdt.train.high'
    low_query_path = high_low_out_datadir + '/query.predict.gbdt.train.low'
high_query_file = open(high_query_path)
low_query_file = open(low_query_path)
print 'high_query_path=',high_query_path
delta_query_quality_filepath_arr=[]
for date in date_arr:#当天的h&l
    if modelmode=='lr':
        delta_query_quality_filepath = hourdatadir + '/query.' + date + '.predict.q.stand'
    else:
        delta_query_quality_filepath = hourdatadir + '/query.'+date+'.gbdt.predict.q'
    delta_query_quality_filepath_arr.append(delta_query_quality_filepath)
    delta_query_quality_file=open(delta_query_quality_filepath)
    print 'delta_query_quality_filepath=',delta_query_quality_filepath
    for line in delta_query_quality_file.readlines():#11470line
        delta_file_line_wc+=1
        fields=line.strip().split('\t')
        if len(fields)<5:
            continue
        query=fields[2]
        quality=float(fields[1])
        query_nicknames=fields[3]
        seg0str=fields[4]
        if query_nicknames=='0':
            query_nicknames=set()
        else:
            query_nicknames_arr=query_nicknames.split('|')
            query_nicknames=set(query_nicknames_arr)
        if quality>=0.5:
            delta_high_dict[query]=(quality,query_nicknames,seg0str)
            if query in delta_low_dict:
                del delta_low_dict[query]
        else:
            delta_low_dict[query]=(quality,query_nicknames,seg0str)
            if query in delta_high_dict:
                del delta_high_dict[query]
    delta_query_quality_file.close()
print 'delta_file_line_wc=',delta_file_line_wc
high_query_file_line_wc=0
if len(date_arr)==1:
    filenameId=date_arr[0]
else:
    filenameId=date_arr[0]+'-'+date_arr[-1]
# error_high_file=open(hourdatadir+'/high_cut.'+modelmode+'.'+filenameId,'w+')
# error_low_file=open(hourdatadir+'/low_cut.'+modelmode+'.'+filenameId,'w+')
for line in high_query_file.readlines():#历史的h
    high_query_file_line_wc+=1
    fields=line.strip().split('\t')
    if len(fields)<2:
        continue
    query=fields[0]
    quality=float(fields[1])
    query_nicknames_set=set()
    seg0str=''
    if len(fields)>=3:
        seg0str = fields[2]
    if len(fields)>=4:
        if query_nicknames_set!='0':
            query_nicknames_set=set(fields[3].split('|'))
    if query in delta_high_dict:
        if len(query_nicknames_set)>0:
            query_val = delta_high_dict[query]
            query_val_nicknames_set = query_val[1]
            for q in query_nicknames_set:
                query_val_nicknames_set.add(q)
            delta_high_dict[query]=(query_val[0],query_val_nicknames_set,query_val[2])
    elif query in delta_low_dict:
    # if query in delta_low_dict:
    #     error_low_file.write(query+'\t'+str(quality)+'\t==>\t'+str(delta_low_dict[query])+'\n')
        if len(query_nicknames_set)>0:
            query_val=delta_low_dict[query]
            query_val_nicknames_set=query_val[1]
            for q in query_nicknames_set:
                query_val_nicknames_set.add(q)
            delta_low_dict[query] = (query_val[0], query_val_nicknames_set,query_val[2])
    else:
    # if query not in delta_low_dict and query not in delta_high_dict:#历史的query没出现在当天
        delta_high_dict[query]=(quality,query_nicknames_set,seg0str)
# error_low_file.close()
print 'high_query_file_line_wc=',high_query_file_line_wc
print 'delta_low_dict wc=',len(delta_low_dict)
print 'delta_high_dict wc=',len(delta_high_dict)
high_query_file.close()
for line in low_query_file.readlines():#历史的l
    fields = line.strip().split('\t')
    if len(fields)<2:
        continue
    query=fields[0]
    quality=float(fields[1])
    query_nicknames_set=set()
    seg0str = ''
    if len(fields)>=3:
        seg0str = fields[2]
    if len(fields)>=4:
        if query_nicknames_set!='0':
            query_nicknames_set=set(fields[3].split('|'))
    if query in delta_high_dict:
        if len(query_nicknames_set) != 0:
            query_val = delta_high_dict[query]
            query_val_nicknames_set = query_val[1]
            for q in query_nicknames_set:
                query_val_nicknames_set.add(q)
            delta_high_dict[query] = (query_val[0], query_val_nicknames_set,query_val[2])
        # error_high_file.write(query + '\t' + str(quality) + '\t==>\t' + str(delta_high_dict[query]) + '\n')
    elif query in delta_low_dict:
    # if query in delta_low_dict:
        if len(query_nicknames_set)!=0:
            query_val = delta_low_dict[query]
            query_val_nicknames_set = query_val[1]
            for q in query_nicknames_set:
                query_val_nicknames_set.add(q)
            delta_low_dict[query] = (query_val[0], query_val_nicknames_set,query_val[2])

    if query not in delta_low_dict and query not in delta_high_dict:#历史的query没出现在当天
        delta_low_dict[query]=(quality,query_nicknames_set,seg0str)
# error_high_file.close()
low_query_file.close()
high_query_path_tempfile=open(high_query_path+'.temp','w+')
low_query_path_tempfile=open(low_query_path+'.temp','w+')
hsortedList=sorted(delta_high_dict.items(),key=lambda d:d[1][0],reverse=True)
lsortedList=sorted(delta_low_dict.items(),key=lambda d:d[1][0],reverse=True)
for k,v in hsortedList:
    high_query_path_tempfile.write(k+'\t'+str(v[0])+'\t'+v[2]+'\t'+'|'.join(v[1])+'\n')
high_query_path_tempfile.close()

for k,v in lsortedList:
    low_query_path_tempfile.write(k+'\t'+str(v[0])+'\t'+v[2]+'\t'+'|'.join(v[1])+'\n')
low_query_path_tempfile.close()

os.remove(high_query_path)
os.remove(low_query_path)
os.rename(high_query_path+'.temp',high_query_path)
os.rename(low_query_path+'.temp',low_query_path)
if deleteFlag==1:
    for onepath in delta_query_quality_filepath_arr:
        os.remove(onepath)