#!/bin/bash


basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;pwd)
echo $proj
pidpath=$proj/bin/pid.hour
echo "PID of the script:"$$
echo $$ > $pidpath
todaystr=`date +%Y%m%d`
sh $proj/scripts/quality/search.query.quality.delta.sh 2 true false >> $proj/logs/search.quality.${todaystr}.log
logdays=`sh $scriptsdir/tool/parseconf.sh logdays`
python $proj/scripts/quality/qualitylogproccessor.py $logdays $proj/logs
rm $pidpath