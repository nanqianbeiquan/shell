#!/bin/bash
EXE_DATE=`date -d "-1 day" +%Y-%m-%d`
TO_DATE=`date  +%Y-%m-%d`
FILE=$1

path=$(cd `dirname $0`; pwd)
cat $FILE|while read param
do

hbase_table=`echo $param|awk -F "|" '{print $1}'`
table_column=`echo $param|awk -F "|" '{print $2}'`
table_name=`echo $param|awk -F "|" '{print $3}'`
hdfs_dir=`echo $param|awk -F "|" '{print $4}'`

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv  -Dimporttsv.separator=$(echo -e '\001') -Dimporttsv.columns=HBASE_ROW_KEY,$table_column $hbase_table $hdfs_dir$table_name


done

