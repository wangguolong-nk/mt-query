#!/bin/bash

basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;cd ..;pwd)
echo 'proj='$proj
scriptsdir=$proj/scripts
libdir=$proj/lib
confdir=$proj/conf
projdatadir=`sh $scriptsdir/tool/parseconf.sh datadir`
echo "scriptsdir="$scriptsdir
echo "projdatadir="$projdatadir

datadir=$projdatadir/work_data/days
if [ $# -lt 1 ];then
        yesterday=`date -d '-1 days' +%Y%m%d`
else
        yesterday=$1
fi
echo "yesterday="$yesterday

#整理数据，同一个uid的query搜索先后顺序
echo "整理数据，同一个uid的query搜索先后顺序"
date
python $scriptsdir/sequential/uid_query_data.py -data_dir=$datadir -dates=$yesterday
if [ $? != 0 ];then
    echo "uid_query_data.py fail."
    exit 2
fi
#计算当天的转移概率
echo "计算当天的转移概率"
date
python $scriptsdir/sequential/sequ_modeling.py -dates=$yesterday -delete=1 -deploydir=$projdatadir
if [ $? != 0 ];then
    echo "sequ_modeling.py fail."
    exit 2
fi
#增量更新模型参数，并生成时序展示结果
echo "增量更新模型参数，并生成时序展示结果"
python $scriptsdir/sequential/modeling_merge.py -delete=1 -deploydir=$projdatadir
if [ $? != 0 ];then
    echo "modeling_merge.py fail."
    exit 2
fi
echo "query时序SUCCESS!"
date