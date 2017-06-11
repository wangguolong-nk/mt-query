# -*- coding:utf-8 -*-
import sys
import os
import datetime

datadir='/data1/wgl/deploy_data/work_data/search/train_data'
# outdir='/data1/wgl/deploy_data/work_data/search/delta_data'
dates=None
delete_flag=0
auto=0
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
    elif argk=='-delete':
        delete_flag=int(argv)
    elif argk=='-auto':
        auto=int(argv)
if auto==1 and not dates:
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    dates = yesterday.strftime('%Y%m%d')
if not dates:
    print 'need PARAMETER: dates!'
    sys.exit(1)
print 'deleta_flag',delete_flag
date_arr=dates.split(',')
outdir=datadir+'/out'
if len(date_arr)==1:
    date_arr = dates.split('-')
    if len(date_arr)==2:
        new_date_arr=[]
        for i in range(int(date_arr[0]),int(date_arr[1])+1):
            new_date_arr.append(str(i))
        date_arr=new_date_arr
print 'date_arr',date_arr
hfilepathdict={}
lfilepathdict={}
for root,dirs,files in os.walk(datadir):
    for file in files:
        hendflag=file.endswith('.vect.high.train')
        lendflag=file.endswith('.vect.low.train')
        if (hendflag or lendflag) and file.startswith('query.'):
            pass
        else:
            continue
        if hendflag:
            datestr=file[len('query.'):-len('.vect.high.train')]
        else:# lendflag:
            datestr=file[len('query.'):-len('.vect.low.train')]
        # print file, type(file),datestr
        fields=datestr.split('-')
        datestr=fields[0]
        date4day=datestr[0:8]
        if date4day not in date_arr:
            continue
        date=int(datestr)
        filepath=root+'/'+file
        if hendflag:
            hfilepathdict[filepath]=date
        else:
            lfilepathdict[filepath]=date
if len(hfilepathdict)==0 or len(lfilepathdict)==0:
    print 'NOTHING'
    sys.exit(0)
hfilepathlist=sorted(hfilepathdict.items(), key=lambda d:d[1], reverse=False)
lfilepathlist=sorted(lfilepathdict.items(), key=lambda d:d[1], reverse=False)
filenameId=date_arr[0]
if len(date_arr)!=1:
    filenameId+='-'+date_arr[-1]
outhpath=outdir+'/query.'+filenameId+'.vect.high.train'
outhfile=open(outhpath,'w+')
print outhpath
delete_paths=[]
for path,date in hfilepathlist:
    rfile=open(path)
    delete_paths.append(path)
    for line in rfile.readlines():
        line = line.strip()
        if line == '':
            continue
        outhfile.write(line+'\n')
outhfile.close()
outlpath=outdir+'/query.'+filenameId+'.vect.low.train'
outlfile=open(outlpath,'w+')
print outlpath
for path,date in lfilepathlist:
    rfile=open(path)
    delete_paths.append(path)
    for line in rfile.readlines():
        line=line.strip()
        if line=='':
            continue
        outlfile.write(line+'\n')
outlfile.close()
for path in delete_paths:
    os.remove(path)
    print 'd:',path