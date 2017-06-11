# -*- coding:utf-8 -*-
import random
import sys

deploy_datadir='/data1/wgl/deploy_data'
mode='l'
for i in range(1,len(sys.argv)):
    arg=sys.argv[i]
    fields=arg.split("=")
    if len(fields)!=2:
        continue
    argk=fields[0]
    argv=fields[1]
    if argk=='-deploy_datadir':
        deploy_datadir=argv
    elif argk=='-mode':
        mode=argv
if not deploy_datadir:
    print 'MUST NEED parameter: deploy_datadir!'
    sys.exit(2)
if mode!='h' and mode!='l':
    print 'parameter: l or h!'
    sys.exit(2)
datapath=deploy_datadir+'/work_data/search/delta_data/query.'+mode+'.sim.out.simple'
datafile=open(datapath)
wc=0
for line in datafile.readlines():
    wc+=1
datafile.close()
print 'line num=',wc
linei_arr=set()
while len(linei_arr)<200:
    r=random.randint(1,wc-1)
    linei_arr.add(r)
sort_i_arr=sorted(linei_arr,reverse=False)
print 'sort_i_arr',sort_i_arr
datafile=open(datapath)
index=0
ii=0
i_inex=sort_i_arr[ii]
for line in datafile:
    if index==i_inex:
        line_fields=line.split('\t')
        newline=line_fields[0]+'\t'+'\t'.join(line_fields[2:])
        newline=newline.strip()
        if newline.endswith(','):
            newline=newline[:-1]
        print '('+str(ii+1)+')',newline,'\n'
        ii+=1
        if ii>=len(sort_i_arr):
            break
        i_inex = sort_i_arr[ii]
    index+=1
datafile.close()