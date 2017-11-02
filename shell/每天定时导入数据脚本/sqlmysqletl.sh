#!/bin.bash
[ "$#" -ne 9 ] && exit 0
## a tool that sync sqlserver table to hadoop hive db made by 
#params nums 8,
#param 1:dest mysql src table ,2:hive dest table,3:hive partition,4:db server,5:connection username,6:connection password,7:split field,8:condition
##


EXE_DATE=`date -d "-1 day" +%Y-%m-%d`
#TO_DATE=`date -d "-2 day" +%Y-%m-%d`
TO_DATE=`date +%Y-%m-%d`

# mysql 数据库操作
MYSQLHOSTNAME="172.16.0.20"  #数据库信息
MYSQLPORT="3306"
MYSQLUSERNAME="root"
MYSQLPASSWORD="LENGjing1@34"

MYSQLDBNAME="job_info"  #数据库名称
MYSQLTABLENAME="job_status" #数据库中表的名称



mysqltable=$1
hivetable=$2
day=$3
dbhost=$4
username=$5
pwd=$6
splitfield=$7
where="$8"
m="$9"
db=`echo $mysqltable|awk -F "." '{print $1}'`
table=`echo $mysqltable|awk -F "." '{print $2}'`


yestday="'"$EXE_DATE"'"
to_day="'"$TO_DATE"'"


condition=`echo $where '>='$yestday' and' $where '<'$to_day `




echo "begin import.. $table"


for k in $(seq 15)
do
hadoop dfs -rmr hdfs://hadoop1:8020/user/root/$table
sqoop import --connect jdbc:mysql://$dbhost:3306/$db --username $username --password $pwd  --table $table  --fields-terminated-by '\001'  --lines-terminated-by '\n' --hive-import  -hive-database ods  --hive-table $db'_'$table  --hive-partition-key dt --hive-partition-value $EXE_DATE --hive-overwrite  --where  "$condition"  --split-by $splitfield  --hive-delims-replacement '<br>' -m $m

insert_sql="insert into ${MYSQLTABLENAME} values('"${table}"','"${TO_DATE}"',now(),'0','')"

echo $insert_sql
mysql -h${MYSQLHOSTNAME} -u${MYSQLUSERNAME}  -p${MYSQLPASSWORD}  ${MYSQLDBNAME} -e "${insert_sql}"

if [ $? -ne 0 ];then
  # echo "$TO_DATE|$table" >> /root/kpi/sqoopdata/sqlserver_etl_error.log
   sleep 60
   continue
fi
break

done

echo "import end $table"

