# -*- coding:utf-8 -*-

datadir='/data1/wgl/meipai-search-suggest-offline-alg-data/quality/work_data/hours'
oldpath=datadir+'/query.20500911.predict.q.stand'
newpath=oldpath+'.new'
swpath='/data1/wgl/meipai-search-suggest-offline-alg-data/quality/work_data/query.nick.new'
swdict={}
depath='/data1/wgl/meipai-search-suggest-offline-alg-data/quality/work_data/query.delete'
defile=open(depath,'w+')
with open(swpath) as fi:
    for line in fi.readlines():
        fields=line.strip().split('\t')
        rawquery=fields[1]
        query1=fields[0]
        query1=query1.lower()
        query1=''.join(query1.split(' '))
        swdict[query1]=rawquery

oldf=open(oldpath)
newf=open(newpath,'w+')
wc=0
for line in oldf.readlines():
    fields=line.strip().split('\t')
    query1=fields[2]
    if query1 not in swdict:
        if wc<10:
            print query1
        defile.write(query1+'\n')
        wc+=1
        continue
    newline=fields[0]+'\t'+fields[1]+'\t'+swdict[query1]+'\t'+fields[3]+'\t'+fields[4]
    newf.write(newline+'\n')
newf.close()
oldf.close()
defile.close()