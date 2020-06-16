#!/bin/bash

#This script is to upgrade based on schema changes

MASTERIP="localhost"
DATABASE="db_log_public"

echo "------> check impala connection <---------"
# param: impala master ip and db name
while true
do
    resNum=`impala-shell -i ${MASTERIP} -d ${DATABASE} -q "select 1" | grep "| 1 |" | wc -l`
    if [ $resNum -ne 2 ] 
    then
        echo "Wait for executing sql ..."
    else
        echo "impala is OK."
        break
    fi
    sleep 5
done

#Add New Columns

impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_app_ctrl add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_app_ctrl as select * from __root_fgt_app_ctrl where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_dlp add columns if not exists (srcdomain string, subservice string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_dlp as select * from __root_fgt_dlp where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_dns add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_dns as select * from __root_fgt_dns where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_emailfilter add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_emailfilter as select * from __root_fgt_emailfilter where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_event add columns if not exists (direction string, cfgname string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_event as select * from __root_fgt_event where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_ips add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_ips as select * from __root_fgt_ips where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_ssh add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_ssh as select * from __root_fgt_ssh where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_ssl add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_ssl as select * from __root_fgt_ssl where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_traffic add columns if not exists (srcdomain string, dstauthserver string, dstgroup string, dstuser string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_traffic as select * from __root_fgt_traffic where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_virus add columns if not exists (srcdomain string, subservice string, cdrcontent string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_virus as select * from __root_fgt_virus where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_waf add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_waf as select * from __root_fgt_waf where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_webfilter add columns if not exists (srcdomain string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_webfilter as select * from __root_fgt_webfilter where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fml_event add columns if not exists (scope string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fml_event as select * from __root_fml_event where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fml_history add columns if not exists (scope string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fml_history as select * from __root_fml_history where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fml_spam add columns if not exists (scope string, user string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fml_spam as select * from __root_fml_spam where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fml_virus add columns if not exists (scope string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fml_virus as select * from __root_fml_virus where adomId = 10001"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table __root_fgt_file_filter add columns if not exists (srcdomain string, subservice string)"
impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view 10001_fgt_file_filter as select * from __root_fgt_file_filter where adomId = 10001"
