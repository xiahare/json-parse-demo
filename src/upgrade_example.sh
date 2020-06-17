#!/bin/bash

# This script is an example to update __root_fgt_traffic and 3_fgt_traffic

MASTERIP=$1
if [ ! -n "$1" ]
then MASTERIP="10.0.1.14"
fi

DATABASE=$2
if [ ! -n "$2" ]
then DATABASE="db_log_public"
fi

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

echo "------ excute impala-shell ---------"
# get all tables
tablestr=`impala-shell -i ${MASTERIP} -d ${DATABASE} -q "show tables"`

array=(${tablestr// | | / })

# This will print all update sqls for the storage group tables and adom views
# Need to know which adoms belong to which storage group
# First, table fgt_traffic as an example
storagegroups=("root")
rootadoms=("3")

for var in ${array[@]}
do
  trimedvar=`echo "$var"`
  varlen=`expr length "$trimedvar"`

  tablesuffix="fgt_traffic" # Take fgt_traffic as an example

  if [ $varlen -gt 2 ] && [ ${trimedvar:0:2} = "__" ]
  #then echo $trimedvar # These are tables
  # fgt_traffic and the new columns should be automatically detected
  then
      if [[ $trimedvar =~ ^\_\_${storagegroups[0]}\_${tablesuffix}$ ]] # Get all storage group fgt_traffic tables with this pattern ^\_\_.*\_fgt_traffic$
      #if [[ $trimedvar =~ ^\_\_.*\_fgt_traffic$ ]]
      then
          #tablename=$trimedvar
          echo "alter table ${trimedvar} add columns if not exists (srcdomain string, dstauthserver string, dstgroup string, dstuser string)"
      fi
  elif [ $varlen -gt 2 ] && [[ $trimedvar =~ ^[0-9]+_* ]]
  #then echo $trimedvar # These are views
  then
      if [[ $trimedvar =~ ^${rootadoms[0]}\_${tablesuffix}$ ]] # Get all adom fgt_traffic views with this pattern ^[0-9]+\_fgt_traffic$
      #if [[ $trimedvar =~ ^.*\_fgt_traffic$ ]]
      then
          # adomId should be parsed out from the view name
          tablename="__${storagegroups[0]}_${tablesuffix}"
          echo "alter view ${trimedvar} as select * from ${tablename} WHERE adomId = ${rootadoms[0]}"
      fi
  fi
done










