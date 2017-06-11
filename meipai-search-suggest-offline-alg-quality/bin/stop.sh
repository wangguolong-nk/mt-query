#!/bin/bash

basepath=$(cd `dirname $0`;pwd)
proj=$(cd $basepath;cd ..;pwd)
pidpath=$proj/bin/pid.hour
if [ ! -f $pidpath ];then
	echo 'start.sh had completed! nothing to do!'
	exit 0
fi
pid=`more $pidpath`
ps --ppid $pid|awk '{if($1~/[0-9]+/) print $1}'|xargs kill -15
if [ $? != 0 ];then
	echo 'kill subpid fail.'
	exit 2
fi
kill -15 $pid
if [ $? != 0 ];then
	echo 'kill pid fail.'
fi
echo 'kill hour.start.sh 0ver'
rm $pidpath
