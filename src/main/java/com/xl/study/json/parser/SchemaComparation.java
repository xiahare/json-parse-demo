package com.xl.study.json.parser;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class SchemaComparation {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		File oldVersion = new File("src/global_mapping_v6.2.json");
		File newVersion = new File("src/global_mapping.json");
		File resultFile = new File("src/schema_change.txt");

		FileUtils.write(resultFile, "\n----Schema Change Summary Start----\r\n", true);
		System.out.println("\n----Schema Change Summary Start----\r\n");

		FileUtils.write(resultFile, "Old version schema is located in: " + oldVersion.getAbsolutePath() + "\r\n", true);
		FileUtils.write(resultFile, "New version schema is located in: " + newVersion.getAbsolutePath() + "\r\n", true);
		System.out.println("Old version schema is located in: " + oldVersion.getAbsolutePath());
		System.out.println("New version schema is located in: " + newVersion.getAbsolutePath() + "\n");

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

//			for ( Object column : columns) {
//				JSONObject columnJson = (JSONObject) column;
//				
//				String colName = columnJson.getString("name") ;
//				String colType = columnJson.getString("type");
//				
//			}

		}

		JSONArray newTables = newJsonObject.getJSONArray("tables");
		for (Object table : newTables) {
			JSONObject tableJson = (JSONObject) table;
			String tableName = tableJson.getString("tableName");
			JSONArray columns = tableJson.getJSONArray("columns");
			newTableMap.put(tableName, columns);
		}

		// Deleted Tables
		int tableCnt = 1;
		for (String oldTable : oldTableMap.keySet()) {
			if (!newTableMap.containsKey(oldTable)) {
				if (tableCnt == 1) {
					FileUtils.write(resultFile, "\n----Deleted Tables----\r\n", true);
					System.out.println("\n----Deleted Tables----\r\n");
				}

				FileUtils.write(resultFile, "[" + tableCnt + "] " + oldTable + "\r\n", true);
				System.out.println("[" + tableCnt + "] " + oldTable);
				tableCnt++;
				continue;
			}
		}

		// New Tables
		tableCnt = 1;
		for (String newTable : newTableMap.keySet()) {
			if (!oldTableMap.containsKey(newTable)) {
				if (tableCnt == 1) {
					FileUtils.write(resultFile, "\n----New Tables----\r\n", true);
					System.out.println("\n----New Tables----\r\n");
				}

				FileUtils.write(resultFile, "[" + tableCnt + "] " + newTable + "\r\n", true);
				System.out.println("[" + tableCnt + "] " + newTable);
				tableCnt++;
				continue;
			}
		}

		// Column Type Change
		tableCnt = 1;
		Map<String, String> columnChangeMap = new LinkedHashMap<String, String>();
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

			for (String colName : columnOldMap.keySet()) {
				if (!columnNewMap.containsKey(colName)) {
					if (!columnChangeMap.containsKey(newTable)) {
						if (tableCnt == 1) {
							FileUtils.write(resultFile, "\n----Table Column Change----\r\n", true);
							System.out.println("\n----Table Column Change----\r\n");
						}
						columnChangeMap.put(newTable, "\n[" + tableCnt + "] " + newTable + "\n====Deleted Columns====");
						tableCnt++;
					}

					JSONObject columnJson = columnOldMap.get(colName);
					// String colIndex = columnJson.getString("index");
					String colType = columnJson.getString("type");
					// String colConverter = columnJson.getString("converter");
					// String colRequired = columnJson.getString("required");

					StringBuilder sb = new StringBuilder(columnChangeMap.get(newTable));
					sb.append("\n    " + colName + "  [" + colType + "]");
					columnChangeMap.put(newTable, sb.toString());
				}
			}

			int count = 1;
			for (String colName : columnNewMap.keySet()) {
				if (!columnOldMap.containsKey(colName)) {
					if (!columnChangeMap.containsKey(newTable)) {
						if (tableCnt == 1) {
							FileUtils.write(resultFile, "\n----Table Column Change----\r\n", true);
							System.out.println("\n----Table Column Change----\r\n");
						}
						columnChangeMap.put(newTable, "\n[" + tableCnt + "] " + newTable + "\n====New Columns====");
						tableCnt++;
					} else if (count == 1) {
						StringBuilder sb = new StringBuilder(columnChangeMap.get(newTable));
						sb.append("\n====New Columns====");
						columnChangeMap.put(newTable, sb.toString());
					}

					JSONObject columnJson = columnNewMap.get(colName);
					// String colIndex = columnJson.getString("index");
					String colType = columnJson.getString("type");
					// String colConverter = columnJson.getString("converter");
					// String colRequired = columnJson.getString("required");

					StringBuilder sb = new StringBuilder(columnChangeMap.get(newTable));
					sb.append("\n    " + colName + "  [" + colType + "]");
					columnChangeMap.put(newTable, sb.toString());
					count++;
				}
			}

			count = 1;
			for (String colName : columnNewMap.keySet()) {
				if (!columnOldMap.containsKey(colName)) {
					continue;
				}

				JSONObject newColumnJson = columnNewMap.get(colName);
				// String newColIndex = newColumnJson.getString("index");
				String newColType = newColumnJson.getString("type");
				// String newColConverter = newColumnJson.getString("converter");
				// String newColRequired = newColumnJson.getString("required");

				JSONObject oldColumnJson = columnOldMap.get(colName);
				// String oldColIndex = oldColumnJson.getString("index");
				String oldColType = oldColumnJson.getString("type");
				// String oldColConverter = oldColumnJson.getString("converter");
				// String oldColRequired = oldColumnJson.getString("required");

				if (!newColType.equals(oldColType)) {
					if (!columnChangeMap.containsKey(newTable)) {
						if (tableCnt == 1) {
							FileUtils.write(resultFile, "\n----Table Column Change----\r\n", true);
							System.out.println("\n----Table Column Change----\r\n");
						}
						columnChangeMap.put(newTable,
								"\n[" + tableCnt + "] " + newTable + "\n====Column Type Change====");
						tableCnt++;
					} else if (count == 1) {
						StringBuilder sb = new StringBuilder(columnChangeMap.get(newTable));
						sb.append("\n====Column Type Change====");
						columnChangeMap.put(newTable, sb.toString());
					}
					StringBuilder sb = new StringBuilder(columnChangeMap.get(newTable));
					sb.append("\n    " + colName + "  [" + oldColType + "->" + newColType + "]");
					columnChangeMap.put(newTable, sb.toString());
					count++;
				}
			}
		}

		for (String key : columnChangeMap.keySet()) {
			FileUtils.write(resultFile, columnChangeMap.get(key), true);
			System.out.println(columnChangeMap.get(key));
		}

		FileUtils.write(resultFile, "\n\n----Schema Change Summary End----\r\n\n", true);
		System.out.println("\n----Schema Change Summary End----\r\n");
	}

}
