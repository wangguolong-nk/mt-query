#!/bin/bash
:<<BLOCK
        query质量计算模型
BLOCK

basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;cd ..;pwd)

scriptsdir=$proj/scripts
libdir=$proj/lib
confdir=$proj/conf
projdatadir=`sh $scriptsdir/tool/parseconf.sh datadir`
envir=`sh $scriptsdir/tool/parseconf.sh envir`
mysql_db_url_mv=`sh $scriptsdir/tool/parseconf.sh mysql_db_url_mv`
mysql_db_mv=`sh $scriptsdir/tool/parseconf.sh mysql_db_mv`
mysql_db_user_mv=`sh $scriptsdir/tool/parseconf.sh mysql_db_user_mv`
mysql_db_pw_mv=`sh $scriptsdir/tool/parseconf.sh mysql_db_pw_mv`
mysql_db_port_mv=`sh $scriptsdir/tool/parseconf.sh mysql_db_port_mv`
quality_predict_querydir=`sh $scriptsdir/tool/parseconf.sh quality_predict_querydir`

if [ $envir == "dev" ];then
	mysql_db_url_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_url_query_test`
	mysql_db_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_query_test`
	mysql_db_user_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_user_query_test`
	mysql_db_pw_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_pw_query_test`
	mysql_db_port_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_port_query_test`
elif [ $envir == "online" ];then
	mysql_db_url_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_url_query_online`
    mysql_db_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_query_online`
    mysql_db_user_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_user_query_online`
    mysql_db_pw_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_pw_query_online`
    mysql_db_port_query=`sh $scriptsdir/tool/parseconf.sh mysql_db_port_query_online`
else
	echo "envir Must is : dev or online!"
	exit 2
fi

#处理参数
export LANG=en_US.UTF-8
dayorhourmode=$1
automode=$2
if [ $dayorhourmode -eq 1 ] ; then
	if $automode ;then
		daydate=`date -d '-1 days' +%Y%m%d`
		startDate=$daydate
        endDate=$daydate
        gray=$3
		if $gray ;then
            gray_val=$4
        fi
	else
		startDate=$3
		endDate=$4
        gray=$5
		if $gray ;then
            gray_val=$6
        fi
	fi
elif [ $dayorhourmode -eq 2 ] ; then
	if $automode ;then
		hourdate=`date -d '-4 hours' +%Y%m%d%H`
		startDate=$hourdate
		endDate=$hourdate
		gray=$3
		if $gray ;then
			gray_val=$4
		fi
	else
		startDate=$3
		endDate=$4
		gray=$5
		if $gray ;then
            gray_val=$6
        fi
	fi
else
	echo 'other'
	exit 2
fi
#step1:下载数据
echo "step1:下载数据-"$startDate
date
if [ $dayorhourmode -eq 2 ];then
	datadir=$projdatadir/work_data/hours
	sh $scriptsdir/common/search.query.down.hours.sh $startDate $endDate $datadir
	if [ $? != 0 ];then
		echo "search.query.down.hours.sh fail."
		exit 2
	fi
else
	datadir=$projdatadir/work_data/search/days
	sh $scriptsdir/common/search.query.down.days.sh $startDate $endDate $datadir
	if [ $? != 0 ];then
        echo "search.query.down.days.sh fail."
        exit 2
    fi
fi
#step2:是否灰度操作并整理
echo "step2:是否灰度操作并整理"
date
if $gray;then
	echo "灰度操作"
	python $scriptsdir/quality/search_query_resort.py -gray_val=$gray_val -dates=$startDate-$endDate -search_data_dir=$datadir -delete=0
	if [ $? != 0 ];then
		echo "灰度操作失败"
		exit 2
	fi
else
	echo "不用灰度操作"
	python $scriptsdir/quality/search_query_resort.py -dates=$startDate-$endDate -search_data_dir=$datadir  -delete=0
	if [ $? != 0 ];then
        echo "search_query_resort.py fail."
        exit 2
    fi
fi
#step3:产生query历史和mvid的详情信息
echo "step3.1:产生query历史详情信息"
date
if [ $envir == "dev" ];then
	deleteQuery=0
	history_search_path=$projdatadir/raw_data/search_word_count.txt
else
	deleteQuery=1
	history_search_path=$datadir/query.${startDate}.history
fi
java -jar $libdir/meitu_query.jar -startDate $startDate -endDate $endDate -datadir $datadir -dburl $mysql_db_url_query -dbname $mysql_db_query -port $mysql_db_port_query -dbuser $mysql_db_user_query -dbpw $mysql_db_pw_query -queryormv 1 -delete $deleteQuery
if [ $? != 0 ];then
	echo '产生query历史详情信息 fail.'
	if [ $envir == "online" ];then
		exit 2
	fi
fi

echo "step3.2:产生mvid的详情信息"
date
java -jar $libdir/meitu_query.jar -startDate $startDate -endDate $endDate -datadir $datadir -dburl=$mysql_db_url -dbname=$mysql_db -port=$mysql_db_port -dbuser=$mysql_db_user -dbpw=$mysql_db_pw
if [ $? != 0 ];then
        echo '产生mvid的详情信息 fail.'
        exit 2
fi
#step4:产生最终的query详情结果
echo "step4:产生最终的query详情结果"
date
python $scriptsdir/quality/finally_query_data.py -startdate=$startDate -enddate=$endDate -datadir=$datadir -delete=1
if [ $? != 0 ];then
        echo 'finally_query_data.py fail.'
        exit 2
fi
#step5:产生样本向量
echo "step5:产生样本向量..."
date
python $scriptsdir/quality/query2vect.py -startdate=$startDate -enddate=$endDate -history_search_path=$history_search_path -datadir=$datadir -delete=1
if [ $? != 0 ];then
        echo 'query2vect.py fail.'
        exit 2
fi
#step6:产生正样本用于训练
echo "step6:产生正样本用于训练(已删除)"

#step7:模型预测query质量
echo "step7:模型预测query质量"
date
python $scriptsdir/quality/lrmodelpredict.py -dates=$startDate-$endDate -datadir=$datadir -mode=1 -delta=1 -modeldir=$projdatadir/model
if [ $? != 0 ];then
        echo 'lrmodelpredict.py fail.'
        exit 2
fi
if [ $envir == "dev" ];then
    #step8:预测结果和老结果整合
    echo "step8:预测结果和老结果整合"
    date
    python $scriptsdir/quality/delta_query_hl.py -dates=$startDate-$endDate -modelmode=lr -hourdatadir=$datadir -high_low_out_datadir=$projdatadir/work_data -quality_predict_querydir=quality_predict_querydir
    if [ $? != 0 ];then
        echo 'delta_query_hl.py fail.'
        exit 2
    fi
fi
#step9:上传query质量结果到es
echo "step9:上传query质量结果到es"
date
java -cp $libdir/meitu_query.jar service.QualityPostHour -dates $startDate-$endDate -delete false -datadir $datadir
if [ $? != 0 ];then
        echo 'QualityPostHuor.java fail.'
        exit 2
fi
date
echo "SUCCESS!"