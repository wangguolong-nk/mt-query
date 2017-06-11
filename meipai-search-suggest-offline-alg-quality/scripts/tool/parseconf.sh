#!/bin/bash

if [ $# -eq 0 ]; then
	echo 'num(parameter) is zero!'
	exit 2
fi

basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;cd ..;pwd)
parameter=$1
conffile=$proj/conf/proj.conf
val=`sed "/^$parameter=/!d;s/.*=//" $conffile`
echo $val