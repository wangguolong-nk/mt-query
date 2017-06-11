# -*- coding:utf-8 -*-
import sys
import os
import re

logdays=int(sys.argv[1])
logdir=sys.argv[2]
if not os.path.exists(logdir):
    print logdir,'not exist'
    sys.exit(1)
logpath_dict={}
pattern='search.similar.(\d+).log'
for root,dirs,filenames in os.walk(logdir):
    if root!=logdir:
        continue
    for filename in filenames:
        m=re.match(pattern,filename)
        if m:
            cur_date=m.group(1)
            logpath_dict[cur_date]=filename
logpath_sorted_list=sorted(logpath_dict.items(),key=lambda d:d[0],reverse=True)
logdate_sorted_list=[v[0] for v in logpath_sorted_list]
if len(logdate_sorted_list)>logdays:
    delete_logpath_arr=logdate_sorted_list[logdays:]
    for delete_logpath in delete_logpath_arr:
        os.remove(logdir+'/'+logpath_dict[delete_logpath])