#!/bin/bash
EXE_DATE=`date -d "-1 day" +%Y-%m-%d`
TO_DATE=`date  +%Y-%m-%d`
[ "$#" -ne 0 ] && EXE_DATE=$1
FILE=$2


path=$(cd `dirname $0`; pwd)
cat $FILE|while read tbinfo
do

hostname=`echo $tbinfo|awk -F "|" '{print $1}'`
username=`echo $tbinfo|awk -F "|" '{print $2}'`
password=`echo $tbinfo|awk -F "|" '{print $3}'`
dbname=`echo $tbinfo|awk -F "|" '{print $4}'`
tablename=`echo $tbinfo|awk -F "|" '{print $5}'`
splitfield=`echo $tbinfo|awk -F "|" '{print $6}'`
appendtime=`echo $tbinfo|awk -F "|" '{print $7}'`
m=`echo $tbinfo|awk -F "|" '{print $9}'`


echo $day
echo "The whole data would import --> $tablename  maps: $m"

sh $path/sqlserveretl.sh  $dbname.$tablename ods.$tablename  0  $hostname $username $password  $splitfield  $appendtime  "$m"


done


