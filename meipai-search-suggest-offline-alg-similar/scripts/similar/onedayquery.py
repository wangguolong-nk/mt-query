# -*- coding:utf-8
import sys
import os

datadir='/data1/wgl/deploy_data/work_data/search/hours'
outdir='/data1/wgl/deploy_data/work_data/search/delta_data'
dates=None
delete_flag=0
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-dates':
        dates=argv
    elif argk=='-datadir':
        datadir=argv
    elif argk=='-outdir':
        outdir=argv
    elif argk=='-delete':
        delete_flag=int(argv)
if not dates:
    print 'need PARAMETER: dates!'
    sys.exit(1)
print 'deleta_flag',delete_flag
date_arr=dates.split(',')
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr
print 'date_arr',date_arr
query_delta_set=set()
hourstr_arr=[]
for i in range(24):
    if i<10:
        hourstr='0'+str(i)
    else:
        hourstr=str(i)
    hourstr_arr.append(hourstr)
cur_hour_paths=[]
for date in date_arr:
    cur_date_query_path_prefix=datadir+'/query.'+date
    for hourstr in hourstr_arr:
        cur_date_query_path=cur_date_query_path_prefix+hourstr
        if not os.path.exists(cur_date_query_path):
            continue
        cur_date_query_file=open(cur_date_query_path)
        cur_hour_paths.append(cur_date_query_path)
        for line in cur_date_query_file.readlines():
            line=line.strip()
            fields=line.split('\t')
            query=fields[0]
            query_delta_set.add(query)
        cur_date_query_file.close()
outfilepath=outdir+'/query.'+date_arr[0]
if len(date_arr)==1 or (len(date_arr)==2 and date_arr[0]==date_arr[1]):
    pass
else:
    outfilepath+='-'+date_arr[-1]
if len(query_delta_set)==0:
    sys.exit(0)
outfile=open(outfilepath,'w+')
for query in query_delta_set:
    outfile.write(query+'\n')
outfile.close()
if delete_flag==1:
    for onepath in cur_hour_paths:
        os.remove(onepath)
