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

public class UpgradeConfigGenerator {

	private static Map<String, String> fieldMapping = new HashMap<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put("StringType", "string");
			put("IntegerType", "int");
			put("LongType", "long");
			put("ShortType", "short");
		}
	};

	private static String MASTER_IP = "10.0.1.14";
	private static String DATABASE_NAME = "db_log_public";
	private static String CATALOG = "_data_catalog";
	private static String CATALOG_ADOM_TABLE = "adom_info";
	private static String CATALOG_TENANT_ID = "tenant_id";
	private static String CATALOG_ADOM_ID = "adom_id";
	private static String CATALOG_STORAGE_ID = "storage_id";

	// Generate an upgrade script of the schema changes
	@SuppressWarnings("deprecation")
	public static void generateUpgradeScript() throws IOException {
		File oldVersion = new File("src/v6.2.2/global_mapping.json");
		File newVersion = new File("src/v6.2.5/global_mapping.json");
		File resultFile = new File("src/upgrade.conf");

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

		FileUtils.write(resultFile, "# upgrade configurations\n\n", true);

		FileUtils.write(resultFile, "# database schemas\n", true);
		FileUtils.write(resultFile, "MASTERIP=" + MASTER_IP + "\n", true);
		FileUtils.write(resultFile, "DATABASE=" + DATABASE_NAME + "\n\n", true);

		FileUtils.write(resultFile, "# adom details are in the table named " + CATALOG + "." + CATALOG_ADOM_TABLE + "\n", true);
		FileUtils.write(resultFile, "CATALOG=" + CATALOG + "\n", true);
		FileUtils.write(resultFile, "CATALOG_ADOM_TABLE=" + CATALOG_ADOM_TABLE + "\n", true);
		FileUtils.write(resultFile, "CATALOG_TENANT_ID=" + CATALOG_TENANT_ID + "\n", true);
		FileUtils.write(resultFile, "CATALOG_ADOM_ID=" + CATALOG_ADOM_ID + "\n", true);
		FileUtils.write(resultFile, "CATALOG_STORAGE_ID=" + CATALOG_STORAGE_ID + "\n\n", true);

		FileUtils.write(resultFile, "# schema changes\n\n", true);

		// Deleted Tables
		for (String oldTable : oldTableMap.keySet()) {
			if (!newTableMap.containsKey(oldTable)) {
				// TODO Write delete table config to file
				continue;
			}
		}

		// New Tables
		for (String newTable : newTableMap.keySet()) {
			if (!oldTableMap.containsKey(newTable)) {
				// TODO Write add table config to file
				continue;
			}
		}

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
				}
			}

			Map<String, JSONObject> columnNewMap = new LinkedHashMap<String, JSONObject>();
			for (Object column : columnsNew) {
				JSONObject columnJson = (JSONObject) column;

				String colName = columnJson.getString("name");
				if (!columnNewMap.containsKey(colName)) {
					columnNewMap.put(colName, columnJson);
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

			// TODO Write delete column config to file

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

		// For now, only support add new column config
		FileUtils.write(resultFile, "# new columns\n", true);
		FileUtils.write(resultFile, "declare -A newColumnsMap\n", true);
		for (String table : columnAddMap.keySet()) {
			// TODO Generate Add Table New Columns Sql
			StringBuilder sb = new StringBuilder("newColumnsMap[\"" + table + "\"]=\"(");

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
		}

	}

	public static void main(String[] args) throws IOException {
		generateUpgradeScript();
	}

}
