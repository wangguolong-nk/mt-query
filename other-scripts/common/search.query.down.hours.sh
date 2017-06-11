#!/bin/bash

:<<BLOCK
        美拍用户搜索播放数据
BLOCK

startHour=$1
endHour=$2
datadir=$3
echo "startHour="$startHour",endHour="$endHour
searchFilePrefix=$datadir/search.user_mv
generateFileList(){
        echo 'generateFileList()...'
        local startHour=$startHour endHour=$endHour
        while [ "$startHour" -le "$endHour" ]
        do
                echo 'curHour='$startHour
                local searchFile=$searchFilePrefix.$startHour
                rm -rf $searchFile
                touch $searchFile
                searchFileList=$searchFileList" "$searchFile
		startHourStr=${startHour:0:8}" "${startHour:8:2}
                startHour=`date -d "+1 hours $startHourStr" +%Y%m%d%H`
        done
}
downloadSearchDate(){
	local startHour=$startHour endHour=$endHour
	while [ "$startHour" -le "$endHour" ]
	do
		local srcFile=$searchFilePrefix.$startHour
		if [ ! -s $srcFile ];then
        		local hivesql="hive-client -e 'select uid,page,unix_timestamp(time),mv_result,client_id,q,core_user_result from meipai where controller_p=\"search\" and action_p=\"user_mv\" and logdate==\"$startHour\" and uid is not null and (client_id=="1089857299" or client_id="1089857302") ' -o $srcFile"
        		echo $hivesql
        		$hivesql
        		if [ $? != 0 ];then
                		echo 'downloadSearchDate(hours) fail.'
                		exit 2
        		fi
		fi
		rm -rf $datadir/search.user_mv-renamed*.$startHour
		startHourStr=${startHour:0:8}" "${startHour:8:2}
		startHour=`date -d "+1 hours $startHourStr" +%Y%m%d%H`
	done
}
date
generateFileList
downloadSearchDate
date
