#!/bin/bash

:<<BLOCK
        美拍用户搜索播放数据
BLOCK

beginDate=$1
endDate=$2
datadir=$3
echo "beginDate="$beginDate",endDate="$endDate
searchFilePrefix=$datadir/search.user_mv
generateFileList(){
        echo 'generateFileList()...'
        local beginDate=$beginDate endDate=$endDate
        while [ "$beginDate" -le "$endDate" ]
        do
                echo 'curDate='$beginDate
                local searchFile=$searchFilePrefix.$beginDate
                rm -rf $searchFile
                touch $searchFile
                searchFileList=$searchFileList" "$searchFile
                beginDate=`date -d "+1 day $beginDate" +%Y%m%d`
        done
}
downloadSearchDate(){
        local beginDate=$beginDate endDate=$endDate
        while [ "$beginDate" -le "$endDate" ]
        do
                echo 'hello'$endDate
                local srcFile=$searchFilePrefix.$beginDate
                local beginValue=`expr $beginDate \* 100`
                local endValue=`expr $beginDate \* 100 + 23`
                if [ ! -s $srcFile ];then
                        local hivesql="hive-client -e 'select uid,page,unix_timestamp(time),mv_result,client_id,q from meipai where controller_p=\"search\" and action_p=\"user_mv\" and logdate>=\"$beginValue\" and logdate<=\"$endValue\" and uid is not null ' -o $srcFile"
                        echo $hivesql
                        $hivesql
                        if [ $? != 0 ];then
                                echo $srcFile' fail.'
                                exit 2
                        fi
                fi
		rm -rf $datadir/search.user_mv-renamed*.$beginDate
                beginDate=`date -d "+1 day $beginDate" +%Y%m%d`
        done
}
generateFileList
downloadSearchDate
date
