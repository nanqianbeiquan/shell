EXE_DATE=`date  +%Y-%m-%d`
ZCH=$1;

hadoop dfs -rmr hdfs://hadoop1:8020/user/root/Shareholder_Info
sqoop import --connect 'jdbc:sqlserver://172.16.0.26;username=leiyubing;password=ljs&*MBG,jsu23;database=pachong'  --table Shareholder_Info  --fields-terminated-by '\001'  --lines-terminated-by '\n' --hive-import --hive-table ods.pachong_shareholder_Info  --hive-partition-key dt --hive-partition-value 'temp'  --hive-overwrite --where  "RegistrationNo='${ZCH}'"    --hive-delims-replacement '<br>'  -m 1

hadoop dfs -rmr hdfs://hadoop1:8020/user/root/keyperson_info
sqoop import --connect 'jdbc:sqlserver://172.16.0.26;username=leiyubing;password=ljs&*MBG,jsu23;database=pachong'  --table keyperson_info  --fields-terminated-by '\001'  --lines-terminated-by '\n' --hive-import --hive-table ods.pachong_keyperson_info  --hive-partition-key dt --hive-partition-value 'temp'  --hive-overwrite --where  "RegistrationNo='${ZCH}'"    --hive-delims-replacement '<br>'  -m 1

hadoop dfs -rmr hdfs://hadoop1:8020/user/root/registered_info
sqoop import --connect 'jdbc:sqlserver://172.16.0.26;username=leiyubing;password=ljs&*MBG,jsu23;database=pachong'  --table registered_info  --fields-terminated-by '\001'  --lines-terminated-by '\n' --hive-import --hive-table ods.pachong_registered_info  --hive-partition-key dt --hive-partition-value 'temp'  --hive-overwrite --where  "RegistrationNo='${ZCH}'"    --hive-delims-replacement '<br>'  -m 1

###生成临时表
hive -f /root/kpi/paddingData.sql


###数据导入hbase
sh  /root/kpi/paddingDataHbase.sh
