# -*- coding:utf-8 -*-
import sys
import os

proj_datadir=sys.argv[1]
proj_datadir=proj_datadir
proj_dir=sys.argv[2]
#判断是否要初始化
init_data_formdir=proj_dir+'/data'
init_flag_path=init_data_formdir+'/init.flag'
if not os.path.exists(init_flag_path):
    print '已经初始化成功过了！'
    sys.exit(0)
dir_levels=proj_datadir.strip().split('/')
dir_levels=[v for v in dir_levels if v!='']
dirstr=''
for i in range(len(dir_levels)-1):
    cur_level=dir_levels[i]
    dirstr+='/'+cur_level
    print dirstr
    if not os.path.exists(dirstr):
        try:
            os.mkdir(dirstr)
        except Exception:
            print 'mkdir data dir fail.'
            sys.exit(1)
dirstr+='/'+dir_levels[-1]
print dirstr
if os.path.exists(dirstr):
    print dirstr,'已存在，请更换工作目录！'
    sys.exit(1)
os.mkdir(dirstr)


#一级目录初始化
first_dirs=['dict','model','work_data']
for first_dir in first_dirs:
    os.mkdir(dirstr+'/'+first_dir)
#初始化数据
init_data_dict_todir=proj_datadir+'/dict'
init_data_model_todir=proj_datadir+'/model'
def copy(srcpath,topath):
    srcf=open(srcpath)
    tof=open(topath,'w+')
    for line in srcf.readlines():
        tof.write(line)
    srcf.close()
    tof.close()
copy(init_data_formdir+'/stopwords.dat',init_data_dict_todir+'/stopwords.dat')
copy(init_data_formdir+'/model.lr.pickle.stand',init_data_model_todir+'/model.lr.pickle.stand')
copy(init_data_formdir+'/model.scaler.pickle.stand',init_data_model_todir+'/model.scaler.pickle.stand')

work_data_dir=dirstr+'/work_data'
copy(init_data_formdir+'/query.predict.train.high.seg',work_data_dir+'/query.predict.train.high.seg')
copy(init_data_formdir+'/query.predict.train.low.seg',work_data_dir+'/query.predict.train.low.seg')
os.mkdir(work_data_dir+'/hours')

os.remove(init_flag_path)
print 'INIT DATA DIR SUCCESS!'