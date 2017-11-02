#!/bin.bash
EXE_DATE=`date -d "-1 day" +%Y-%m-%d`
[ "$#" -ne 0 ] && EXE_DATE=$1
FILE=$2

path=$(cd `dirname $0`; pwd)

#解析裁判文书第一步
java -jar /apps/likai/LengJingETL.jar --jobName=TransTextToSequenceFile --dt=${EXE_DATE}


#sqoop 导入数据

######## sqoop导入工商数据
sh $path/doGetTable.sh $EXE_DATE "$path/table.list.prod"  


###########sqoop导入司法数据
sh $path/doGetTable.sh $EXE_DATE "$path/table.listsifa.prod"
sh $path/doMySqlGetTable.sh $EXE_DATE "$path/table.listmysql.prod"



# 生成临时表数据

sh $path/loadTempTable.sh


# 把数据加载到hbase表内

hadoop jar /apps/likai/LengJingETL.jar --jobName=DeleteFromLengJingGS --dt=${EXE_DATE}

#sh $path/loadDataTable.sh "$path/table.hbase.prod"

sh $path/loadDataTable.sh "$path/table.hbase.prod2"


#加载top图
#hadoop jar /apps/likai/LengJingETL.jar --jobName=AddBaseToGraph --dt=${EXE_DATE}
#hadoop jar /apps/likai/LengJingETL.jar --jobName=AddGdToGraph --dt=${EXE_DATE}
#hadoop jar /apps/likai/LengJingETL.jar --jobName=AddBaxxToGraph --dt=${EXE_DATE}

