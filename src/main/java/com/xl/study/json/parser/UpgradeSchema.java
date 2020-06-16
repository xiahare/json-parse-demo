package com.xl.study.json.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class UpgradeSchema {

	private static Map<String, String> fieldMapping = new HashMap<String, String>() {{
        put("StringType", "string");
        put("IntegerType", "int");
        put("LongType", "long");
        put("ShortType", "short");
    }};
	
    private static String MASTER_IP = "10.0.1.14";
    private static String DATABASE_NAME = "db_log_public";
    
	// Generate an upgrade script of the schema changes
	public static void generateUpgradeScript(Map<String, List<String>> storageGroupAdoms) throws IOException {
		File oldVersion = new File("src/v6.2.2/global_mapping.json");
		File newVersion = new File("src/v6.2.5/global_mapping.json");
		File resultFile = new File("src/upgrade.sh");

		// For now, only consider add/delete tables and add new columns
		String oldJsonStr = FileUtils.readFileToString(oldVersion, "UTF-8");
		JSONObject oldJsonObject = JSON.parseObject(oldJsonStr);

		String newJsonStr = FileUtils.readFileToString(newVersion, "UTF-8");
		JSONObject newJsonObject = JSON.parseObject(newJsonStr);

		Map<String, JSONArray> oldTableMap = new LinkedHashMap<String, JSONArray>();
		Map<String, JSONArray> newTableMap = new LinkedHashMap<String, JSONArray>();
		
		JSONArray oldTables = oldJsonObject.getJSONArray("tables");
		for (Object table : oldTables) {
			JSONObject tableJson = (JSONObject) table;
			String tableName = tableJson.getString("tableName");
			JSONArray columns = tableJson.getJSONArray("columns");
			oldTableMap.put(tableName, columns);
		}

		JSONArray newTables = newJsonObject.getJSONArray("tables");
		for (Object table : newTables) {
			JSONObject tableJson = (JSONObject) table;
			String tableName = tableJson.getString("tableName");
			JSONArray columns = tableJson.getJSONArray("columns");
			newTableMap.put(tableName, columns);
		}

		// TODO Only create script when there exists schema change
		FileUtils.write(resultFile, "#!/bin/bash\n\n", true);

		FileUtils.write(resultFile, "#This script is to upgrade based on schema changes\n\n", true);
		
		// TODO Handle input MASTERIP and DATABASE
		FileUtils.write(resultFile, "MASTERIP=\"" + MASTER_IP + "\"\n", true);
		FileUtils.write(resultFile, "DATABASE=\"" + DATABASE_NAME + "\"\n\n", true);

		// Check impala connection
		FileUtils.write(resultFile, "echo \"------> check impala connection <---------\"\n", true);
		FileUtils.write(resultFile, "# param: impala master ip and db name\n", true);
		FileUtils.write(resultFile, "while true\n", true);
		FileUtils.write(resultFile, "do\n", true);
		FileUtils.write(resultFile, "    resNum=`impala-shell -i ${MASTERIP} -d ${DATABASE} -q \"select 1\" | grep \"| 1 |\" | wc -l`\n", true);
		FileUtils.write(resultFile, "    if [ $resNum -ne 2 ] \n", true);
		FileUtils.write(resultFile, "    then\n", true);
		FileUtils.write(resultFile, "        echo \"Wait for executing sql ...\"\n", true);
		FileUtils.write(resultFile, "    else\n", true);
		FileUtils.write(resultFile, "        echo \"impala is OK.\"\n", true);
		FileUtils.write(resultFile, "        break\n", true);
		FileUtils.write(resultFile, "    fi\n", true);
		FileUtils.write(resultFile, "    sleep 5\n", true);
		FileUtils.write(resultFile, "done\n\n", true);

//		FileUtils.write(resultFile, "# get all tables\n", true);
//		FileUtils.write(resultFile, "tablestr=`impala-shell -i ${MASTERIP} -d ${DATABASE} -q \"show tables\"`\n\n", true);
//		FileUtils.write(resultFile, "array=(${tablestr// | | / })\n\n", true);

		// Deleted Tables
		for (String oldTable : oldTableMap.keySet()) {
			if (!newTableMap.containsKey(oldTable)) {
				// TODO Add sql to delete this oldTable
				// DROP TABLE database.<oldTable> ...
				// DROP VIEW ...
				//FileUtils.write(resultFile, "impala-shell -i ${MASTERIP} -d ${DATABASE} -q \"drop table __" + storageGroup + "_" + oldTable, true);
				continue;
			}
		}

		// New Tables
		for (String newTable : newTableMap.keySet()) {
			if (!oldTableMap.containsKey(newTable)) {
				// TODO Add sql to add this newTable
				// TODO Consider PK fields, partition by fields, partition number, kudu, replica, etc.
				// CREATE TABLE database.<newTable> ... PK, partition by, stored as kudu, replica, etc.
				// CREATE VIEW
				continue;
			}
		}

		FileUtils.write(resultFile, "#Add New Columns\n\n", true);
		// Column Changes (Add, Delete, Update, Type Change, etc.)
		
		// Key: TableName
		// Values: Columns to be deleted
		Map<String, List<String>> columnDeleteMap = new LinkedHashMap<String, List<String>>();
		
		// Key: TableName
		// Values: Column&Type Pairs to be added
		Map<String, Map<String, String>> columnAddMap = new LinkedHashMap<String, Map<String, String>>();
		
		for (String newTable : newTableMap.keySet()) {
			if (!oldTableMap.containsKey(newTable)) {
				continue;
			}

			JSONArray columnsNew = newTableMap.get(newTable);
			JSONArray columnsOld = oldTableMap.get(newTable);

			Map<String, JSONObject> columnOldMap = new LinkedHashMap<String, JSONObject>();
			for (Object column : columnsOld) {
				JSONObject columnJson = (JSONObject) column;

				String colName = columnJson.getString("name");
				if (!columnOldMap.containsKey(colName)) {
					columnOldMap.put(colName, columnJson);
				} else {
					// TODO Handle duplicate columns
					System.out.println("Duplicate column names found in old version " + newTable + ". Please verify!");
				}
			}

			Map<String, JSONObject> columnNewMap = new LinkedHashMap<String, JSONObject>();
			for (Object column : columnsNew) {
				JSONObject columnJson = (JSONObject) column;

				String colName = columnJson.getString("name");
				if (!columnNewMap.containsKey(colName)) {
					columnNewMap.put(colName, columnJson);
				} else {
					// TODO Handle duplicate columns
					System.out.println("Duplicate column names found in new version " + newTable + ". Please verify!");
				}
			}

			List<String> deletedColumns = new ArrayList<String>();
			for (String colName : columnOldMap.keySet()) {
				if (!columnNewMap.containsKey(colName)) {
					deletedColumns.add(colName);
					if (!columnDeleteMap.containsKey(newTable)) {
						columnDeleteMap.put(newTable, deletedColumns);
					}
				}
			}
			if (!deletedColumns.isEmpty()) {
				columnDeleteMap.put(newTable, deletedColumns);
			}

			// TODO Generate sql based on columnDeleteMap
			
			Map<String, String> newColumns = new LinkedHashMap<String, String>();
			for (String colName : columnNewMap.keySet()) {
				if (!columnOldMap.containsKey(colName)) {
					JSONObject columnJson = columnNewMap.get(colName);
					String colType = columnJson.getString("type");
					newColumns.put(colName, colType);
				}
			}
			if (!newColumns.isEmpty()) {
				columnAddMap.put(newTable, newColumns);
			}
		}
		
		// Generate sql based on new columns
		// Need to know the storage groups and adoms
		// For example, 
		// (1) alter table __root_test_fct_netscan add if not exists columns (testintcolumn1 int, testintcolumn2 int);
		// (2) alter view 10001_test_fct_netscan as select * from __root_test_fct_netscan where adomId = 10001;
		
		for (String storageGroup : storageGroupAdoms.keySet()) {
			for (String table : columnAddMap.keySet()) {
				// TODO Generate Add Table New Columns Sql
				StringBuilder sb = new StringBuilder("impala-shell -i ${MASTERIP} -d ${DATABASE} -q \"alter table __" + storageGroup + "_" + table + " add columns if not exists (");

				Map<String, String> newColumns = columnAddMap.get(table);
				for (String columnName : newColumns.keySet()) {
					String columnType = newColumns.get(columnName);
					if (fieldMapping.containsKey(columnType)) {
						columnType = fieldMapping.get(columnType);
					}
					// TODO This column type is StringType should map it to string
					sb.append(columnName + " " + columnType + ", ");
				}
				
				if (sb.length() >= 2) {
					sb.delete(sb.length() - 2, sb.length());
					sb.append(")\"");
				}

				FileUtils.write(resultFile, sb.toString() + "\n", true);
				
				List<String> adoms = storageGroupAdoms.get(storageGroup);
				for (String adom : adoms) {
					// TODO Generate Add View New Columns Sql
					StringBuilder sb2 = new StringBuilder("impala-shell -i ${MASTERIP} -d ${DATABASE} -q \"alter view " + adom + "_" + table + " as select * from __" + storageGroup + "_" + table + " where adomId = " + adom + "\"");
					FileUtils.write(resultFile, sb2.toString() + "\n", true);
				}
			}
		}

	}

	public static void main(String[] args) throws IOException {
		Map<String, List<String>> storageGroupAdoms = new LinkedHashMap<String, List<String>>();
		List<String> adoms = new ArrayList<String>();
		adoms.add("10001");
		storageGroupAdoms.put("root", adoms);
		generateUpgradeScript(storageGroupAdoms);
	}

}
