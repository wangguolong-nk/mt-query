import os
projdatadir='/data1/wgl/deploy_data'
datadir=projdatadir+'/work_data/search/delta_data'
simoutpath=datadir+'/query.sim.20170519'
ed_dict={}
with open(simoutpath) as fi:
    for line in fi.readlines():
        line=line.strip()
        fields=line.split('\t')
        ed_dict[fields[0]]=line
delta_querysimoutpath=projdatadir+'/work_data/search/delta_data/query.sim.delta'
if not os.path.exists(delta_querysimoutpath):
    f=open(delta_querysimoutpath,'w+')
    f.close()
else:
    with open(delta_querysimoutpath) as fi:
        for line in fi.readlines():
            line = line.strip()
            fields = line.split('\t')
            ed_dict[fields[0]] = line
with open(delta_querysimoutpath+'.del','w+') as fo:
    for query in ed_dict:
        fo.write(ed_dict[query]+'\n')
os.remove(delta_querysimoutpath)
os.rename(delta_querysimoutpath+'.del',delta_querysimoutpath)