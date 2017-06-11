#!/bin/bash

basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;cd ..;pwd)
scriptsdir=$proj/scripts
libdir=$proj/lib
confdir=$proj/conf
projdatadir=`sh $scriptsdir/tool/parseconf.sh datadir`
quality_datadir=`sh $scriptsdir/tool/parseconf.sh quality_datadir`

if [ $# == 0 ];then
        yesterday=`date -d '-1 days' +%Y%m%d`
else
        yesterday=$1
fi
echo "开始追加更新"$yesterday"的相似结果..."

datadir=$projdatadir/work_data/days
#收集每天的quality向量，用于后期优化建模
echo "收集每天的quality向量，用于后期优化建模"
python $scriptsdir/daily/queryvect4quality_daily.py -date=$yesterday -projdatadir=$proj -qualitydatadir=$quality_datadir -delete=1
if [ $? != 0 ];then
    echo "queryvect4quality_daily.py fail."
fi
#每天的query收集
echo "下载"$yesterday"的数据并提取query"
sh $scriptsdir/common/search.query.down.days.sh $yesterday $yesterday $datadir
if [ $? != 0 ];then
    echo "下载"$yesterday"的数据 fail."
    exit 2
fi
#整理出query
echo "整理出query"
python $scriptsdir/similar/extract_query.py -dates=$yesterday -data_dir=$datadir -delete=0
if [ $? == 0 ];then #success
    #query转w2v向量
    echo "query转w2v向量"
    python $scriptsdir/similar/queryvect4w2v.py -dates=$yesterday -projdatadir=$projdatadir
    if [ $? == 0 ];then
        echo "queryvect4w2v.py ok."
        #语义相似
		echo "语义相似"
		python $scriptsdir/similar/querysimbyw2v.py -dates=$yesterday -projdatadir=$projdatadir -delete=0
		if [ $? != 0 ];then
			echo "querysimbyw2v.py fail."
		else
		    #w2v相似增量更新
		    echo "w2v相似增量更新"
		    python $scriptsdir/similar/w2vsimout_delta_meger.py -deploydir=$projdatadir -date=$yesterday -delete=0
		    if [ $? != 0 ];then
		        echo "w2vsimout_delta_meger.py fail."
		    else
		        echo "最终相似结果"
		        python $scriptsdir/similar/querysimout.py -deploydir=$projdatadir
		        if [ $? != 0 ];then
		            echo "querysimout.py fail."
		        fi
		    fi
		fi
		#query时序模型
		echo "query时序模型"
		sh $scriptsdir/sequential/daily_sequential.sh $yesterday
		if [ $? != 0 ];then
		    echo "query时序模型: daily_sequential.sh fail."
		fi
    else
        echo "queryvect4w2v.py fail."
    fi
    #编辑距离
    echo "编辑距离"
    python -u $scriptsdir/similar/editdist_l2h_daily.py -mode=1 -date=$yesterday -projdatadir=$projdatadir
    if [ $? != 0 ];then
		echo "editdist_l2h_daily.py fail."
	fi
else
    echo "extract_query.py fail."
fi
#最终相似结果整合
echo "最终相似结果整合"
python $scriptsdir/similar/querysimout.py -deploydir=$projdatadir
if [ $? != 0 ];then
    echo "querysimout.py fail."
else
    echo "SUCCESS!"
fi
date
