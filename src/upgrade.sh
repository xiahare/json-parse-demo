#!/bin/bash

echo "------ read configurations ---------"
source upgrade.conf

# params to be read from a config file
if [ ! -n "$MASTERIP" ]
then echo "MASTERIP must be set"
break
fi

if [ ! -n "$DATABASE" ]
then echo "DATABASE must be set"
break
fi

if [ ! -n "$CATALOG" ]
then echo "CATALOG must be set"
break
fi

echo "------> check database connection <---------"
while true
do
    resNum=`impala-shell -i ${MASTERIP} -d ${DATABASE} -q "select 1" | grep "| 1 |" | wc -l`
    if [ $resNum -ne 2 ]
    then
        echo "Testing impala ${DATABASE} connection ..."
    else
        echo "${DATABASE} is OK."
        break
    fi
    sleep 5
done

while true
do
    resNum=`impala-shell -i ${MASTERIP} -d ${CATALOG} -q "select 1" | grep "| 1 |" | wc -l`
    if [ $resNum -ne 2 ]
    then
        echo "Testing impala ${CATALOG} connection ..."
    else
        echo "${CATALOG} is OK."
        break
    fi
    sleep 5
done

echo "------ retrieve storage groups and adoms ---------"

declare -A adomInfoMap

adomInfoRows=`impala-shell -i ${MASTERIP} -d ${CATALOG} -q "select concat(${CATALOG_ADOM_ID}, '_', ${CATALOG_STORAGE_ID}) from ${CATALOG_ADOM_TABLE} where ${CATALOG_TENANT_ID}='${DATABASE}'"`

storageAdoms=(${adomInfoRows// | | / })
for storageAdom in ${storageAdoms[@]}
do
  trimedStorageAdom=`echo "$storageAdom"`
  if [[ $trimedStorageAdom =~ ^[0-9]+_* ]]
  then
      #echo $trimedStorageAdom
      tempArray=(${trimedStorageAdom//_/ })
      adomInfoMap[${tempArray[0]}]=${tempArray[1]}
  fi
done

echo "------ retrieve physical tables ---------"
# get all tables
tablestr=`impala-shell -i ${MASTERIP} -d ${DATABASE} -q "show tables"`
array=(${tablestr// | | / })

echo "------ upgrade start ---------"
# for now, only support adding new columns

for var in ${array[@]}
do
  trimedvar=`echo "$var"`
  varlen=`expr length "$trimedvar"`

  if [ $varlen -gt 2 ] && [[ $trimedvar =~ ^\_\_(.*)\_(.*)[^\_][^v][^i][^e][^w]$ ]]
  then
      tempVar=`echo ${BASH_REMATCH[0]}` # such as __root_fgt_traffic
      tabTokens=(${tempVar//_/ }) # split with underscore
      tabTokenLen=${#tabTokens[@]} # element num
      storageId=${tabTokens[0]} # storage id

      logDefinition="${tabTokens[1]}"
      for((i=2;i<tabTokenLen;i++));do
          logDefinition="${logDefinition}_${tabTokens[i]}" # assembly log def like fgt_traffic
      done

      tempVal="${newColumnsMap[${logDefinition}]}"
      if [ -n "$tempVal" ]
      then
          echo "alter table ${trimedvar} add columns if not exists ${tempVal}"
          echo "alter view ${trimedvar}_view as select * from ${trimedvar}" # for example, __root_fgt_traffic_view
          #impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter table ${trimedvar} add columns if not exists ${tempVal}"
          #impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view ${trimedvar}_view as select * from ${trimedvar}"
      fi
  fi
done

for var in ${array[@]}
do
  trimedvar=`echo "$var"`
  varlen=`expr length "$trimedvar"`

  if [ $varlen -gt 2 ] && [[ $trimedvar =~ ^[0-9]+_* ]]
  then
      viewTokens=(${trimedvar//_/ })
      viewTokenLen=${#viewTokens[@]} # element num
      viewAdom=${viewTokens[0]}

      logDefinition="${viewTokens[1]}"
      for((i=2;i<viewTokenLen;i++));do
          logDefinition="${logDefinition}_${viewTokens[i]}" # assembly log def like fgt_traffic
      done

      tempVal1="${newColumnsMap[${logDefinition}]}"
      tempVal2="${adomInfoMap[${viewAdom}]}"
      if [ -n "$tempVal1" ] && [ -n "$tempVal2" ]
      then
          tablename="__${tempVal2}_${logDefinition}"
          echo "alter view ${trimedvar} as select * from ${tablename} WHERE adomId = ${viewAdom}"
          #impala-shell -i ${MASTERIP} -d ${DATABASE} -q "alter view ${trimedvar} as select * from ${tablename} WHERE adomId = ${viewAdom}"
      fi
  fi
done

echo "------ upgrade end ---------"
