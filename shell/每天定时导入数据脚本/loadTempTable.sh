#!/bin.bash

EXE_DATE=`date -d "-1 day" +%Y-%m-%d`
hive -hiveconf dt=${EXE_DATE} -f /apps/likai/tempGSForHbase.sql


hive -hiveconf dt=${EXE_DATE} -f /apps/likai/tempGSForHbase2.sql

hive -hiveconf dt=${EXE_DATE} -f /apps/likai/tempSFForHbase.sql
