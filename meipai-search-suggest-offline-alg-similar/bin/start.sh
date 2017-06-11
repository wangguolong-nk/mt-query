#!/bin/bash


basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;pwd)
echo $proj
pidpath=$proj/bin/pid.day
echo "PID of the script:"$$
echo $$ > $pidpath
sh $proj/scripts/daily/daily_query.sh >> $proj/logs/search.similar.${todaystr}.log
logdays=`sh $scriptsdir/tool/parseconf.sh logdays`
python $proj/scripts/tool/similarlogproccessor.py $logdays $proj/logs
rm $pidpath