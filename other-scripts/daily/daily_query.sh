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

yesterday=`date -d '-1 days' +%Y%m%d`
#收集每天的query的vect用于query quality训练
echo "收集每天的query的vect用于query quality训练:"$yesterday
python  $scriptsdir/daily/queryvect4quality_daily.py -date=$yesterday -delete=1 -projdatadir=$projdatadir
if [ $? != 0 ];then
    echo "queryvect4quality_daily.py fail."
fi


#每天的query收集
echo "每天的query收集"
python $scriptsdir/similar/w2v/onedayquery.py -dates=$yesterday -delete=1 -datadir=$projdatadir/work_data/search/hours -outdir=$projdatadir/work_data/search/delta_data
if [ $? == 0 ];then
	#query转w2v向量
	echo "query转w2v向量"
	python $scriptsdir/daily/queryvect4sim_dailyquery.py -dates=$yesterday -projdatadir=$projdatadir
	if [ $? == 0 ];then  #success
		#语义相似
		echo "语义相似"
		python $scriptsdir/daily/querysim_daily.py -dates=$yesterday -projdatadir=$projdatadir -delete=1
		if [ $? != 0 ];then
			echo "querysim_daily.py fail."
		fi
		#query时序模型
        echo "query时序模型"
        sh $scriptsdir/daily/daily_sequential.sh $yesterday
        if [ $? != 0 ];then
            echo "query时序模型: daily_sequential.sh fail."
        fi
	else
		echo "queryvect4sim_dailyquery.py fail."
	fi
	#编辑距离
    echo "编辑距离"
	python -u $scriptsdir/daily/editdist_l2h_daily.py -mode=1 -date=$yesterday -projdatadir=$projdatadir
	if [ $? != 0 ];then
		echo "editdist_l2h_daily.py fail."
	fi
else
	echo 'onedayquery.py fail.'
fi

echo "SUCCESS!"
date
