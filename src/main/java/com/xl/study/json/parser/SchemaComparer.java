package com.xl.study.json.parser;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class SchemaComparer {
	private static Map<String, File> getAllFiles(String fileDir) {
		Map<String, File> fileMap = new HashMap<String, File>();
        File file = new File(fileDir);
        File[] files = file.listFiles();
        if (files == null) {
            return fileMap;
        }

        for (File f : files) {
            if (f.isFile()) {
                fileMap.put(f.getName(), f);
            } else if (f.isDirectory()) {
                getAllFiles(f.getAbsolutePath());
            }
        }

        return fileMap;
    }

	@SuppressWarnings("deprecation")
	public static void compareDiff(String oldDir, String nowDir) throws IOException {

		Map<String, File> oldFiles = getAllFiles(oldDir);
		Map<String, File> newFiles = getAllFiles(nowDir);

		File resultFile = new File("src/schema_change.txt");
		FileUtils.write(resultFile, "\n----Schema Change Summary Start----\r\n", true);
		System.out.println("\n----Schema Change Summary Start----\r\n");

		FileUtils.write(resultFile, "Old version schema is located in: " + oldDir + "\r\n", true);
		FileUtils.write(resultFile, "New version schema is located in: " + nowDir + "\r\n", true);
		System.out.println("Old version schema is located in: " + oldDir);
		System.out.println("New version schema is located in: " + nowDir + "\n");

		int tableCnt = 1;
		for (String newFile : newFiles.keySet()) {
			if (!oldFiles.containsKey(newFile)) {
				if (tableCnt == 1) {
					FileUtils.write(resultFile, "\n----New Tables----\r\n", true);
					System.out.println("\n----New Tables----\r\n");
				}

				File newVersion = newFiles.get(newFile);
				String newJsonStr = FileUtils.readFileToString(newVersion, "UTF-8");
				JSONObject newJsonObject = JSON.parseObject(newJsonStr);
				String newTableName = newJsonObject.getString("tableName");
				//JSONArray newColumns = newJsonObject.getJSONArray("columns");

				FileUtils.write(resultFile, "[" + tableCnt + "] " + newTableName + "\r\n", true);
				System.out.println("[" + tableCnt + "] " + newTableName);
				tableCnt++;
				continue;
			}
		}

		tableCnt = 1;
		for (String oldFile : oldFiles.keySet()) {
			if (!newFiles.containsKey(oldFile)) {
				if (tableCnt == 1) {
					FileUtils.write(resultFile, "\n----Deleted Tables----\r\n", true);
					System.out.println("\n----Deleted Tables----\r\n");
				}

				File oldVersion = oldFiles.get(oldFile);
				String oldJsonStr = FileUtils.readFileToString(oldVersion, "UTF-8");
				JSONObject oldJsonObject = JSON.parseObject(oldJsonStr);
				String oldTableName = oldJsonObject.getString("tableName");
				//JSONArray oldColumns = oldJsonObject.getJSONArray("columns");

				FileUtils.write(resultFile, "[" + tableCnt + "] " + oldTableName + "\r\n", true);
				System.out.println("[" + tableCnt + "] " + oldTableName);
				tableCnt++;
				continue;
			}
		}

		tableCnt = 1;
		Map<String, String> columnChangeMap = new LinkedHashMap<String, String>();
		for (String newFile : newFiles.keySet()) {
			if (!oldFiles.containsKey(newFile) || newFile.equals("tables.json")) {
				continue;
			}
			File newVersion = newFiles.get(newFile);
			String newJsonStr = FileUtils.readFileToString(newVersion, "UTF-8");
			JSONObject newJsonObject = JSON.parseObject(newJsonStr);
			String newTableName = newJsonObject.getString("tableName");
			JSONArray columnsNew = newJsonObject.getJSONArray("columns");

			File oldVersion = oldFiles.get(newFile);
			String oldJsonStr = FileUtils.readFileToString(oldVersion, "UTF-8");
			JSONObject oldJsonObject = JSON.parseObject(oldJsonStr);
			String oldTableName = oldJsonObject.getString("tableName");
			JSONArray columnsOld = oldJsonObject.getJSONArray("columns");

			if (!newTableName.equals(oldTableName)) {
				// TODO Corner case that inside table name is different
			}

			Map<String, JSONObject> columnOldMap = new HashMap<String, JSONObject>();
			for (Object column : columnsOld) {
				JSONObject columnJson = (JSONObject) column;

				String colName = columnJson.getString("name");
				if (!columnOldMap.containsKey(colName)) {
					columnOldMap.put(colName, columnJson);
				} else {
					// TODO Handle duplicate columns
					System.out.println("Duplicate column names found in old version " + oldTableName + ". Please verify!");
				}
			}

			Map<String, JSONObject> columnNewMap = new HashMap<String, JSONObject>();
			for (Object column : columnsNew) {
				JSONObject columnJson = (JSONObject) column;

				String colName = columnJson.getString("name");
				if (!columnNewMap.containsKey(colName)) {
					columnNewMap.put(colName, columnJson);
				} else {
					// TODO Handle duplicate columns
					System.out.println("Duplicate column names found in new version " + newTableName + ". Please verify!");
				}
			}

			for (String colName : columnOldMap.keySet()) {
				if (!columnNewMap.containsKey(colName)) {
					if (!columnChangeMap.containsKey(newTableName)) {
						if (tableCnt == 1) {
							FileUtils.write(resultFile, "\n----Table Column Change----\r\n", true);
							System.out.println("\n----Table Column Change----\r\n");
						}
						columnChangeMap.put(newTableName, "\n[" + tableCnt + "] " + newTableName + "\n====Deleted Columns====");
						tableCnt++;
					}

					JSONObject columnJson = columnOldMap.get(colName);
					//String colIndex = columnJson.getString("index");
					String colType = columnJson.getString("type");
					//String colConverter = columnJson.getString("converter");
					//String colRequired = columnJson.getString("required");

					StringBuilder sb = new StringBuilder(columnChangeMap.get(newTableName));
					sb.append("\n    " + colName + "  [" + colType + "]");
					columnChangeMap.put(newTableName, sb.toString());
				}
			}

			int count = 1;
			for (String colName : columnNewMap.keySet()) {
				if (!columnOldMap.containsKey(colName)) {
					if (!columnChangeMap.containsKey(oldTableName)) {
						if (tableCnt == 1) {
							FileUtils.write(resultFile, "\n----Table Column Change----\r\n", true);
							System.out.println("\n----Table Column Change----\r\n");
						}
						columnChangeMap.put(newTableName, "\n[" + tableCnt + "] " + newTableName + "\n====New Columns====");
						tableCnt++;
					} else if (count == 1) {
						StringBuilder sb = new StringBuilder(columnChangeMap.get(newTableName));
						sb.append("\n====New Columns====");
						columnChangeMap.put(newTableName, sb.toString());
					}

					JSONObject columnJson = columnNewMap.get(colName);
					//String colIndex = columnJson.getString("index");
					String colType = columnJson.getString("type");
					//String colConverter = columnJson.getString("converter");
					//String colRequired = columnJson.getString("required");

					StringBuilder sb = new StringBuilder(columnChangeMap.get(newTableName));
					sb.append("\n    " + colName + "  [" + colType + "]");
					columnChangeMap.put(newTableName, sb.toString());
					count++;
				}
			}

			count = 1;
			for (String colName : columnNewMap.keySet()) {
				if (!columnOldMap.containsKey(colName)) {
					continue;
				}

				JSONObject newColumnJson = columnNewMap.get(colName);
				//String newColIndex = newColumnJson.getString("index");
				String newColType = newColumnJson.getString("type");
				//String newColConverter = newColumnJson.getString("converter");
				//String newColRequired = newColumnJson.getString("required");

				JSONObject oldColumnJson = columnOldMap.get(colName);
				//String oldColIndex = oldColumnJson.getString("index");
				String oldColType = oldColumnJson.getString("type");
				//String oldColConverter = oldColumnJson.getString("converter");
				//String oldColRequired = oldColumnJson.getString("required");

				if (!newColType.equals(oldColType)) {
					if (!columnChangeMap.containsKey(newTableName)) {
						if (tableCnt == 1) {
							FileUtils.write(resultFile, "\n----Table Column Change----\r\n", true);
							System.out.println("\n----Table Column Change----\r\n");
						}
						columnChangeMap.put(newTableName, "\n[" + tableCnt + "] " + newTableName + "\n====Column Type Change====");
						tableCnt++;
					} else if (count == 1) {
						StringBuilder sb = new StringBuilder(columnChangeMap.get(newTableName));
						sb.append("\n====Column Type Change====");
						columnChangeMap.put(newTableName, sb.toString());
					}
					StringBuilder sb = new StringBuilder(columnChangeMap.get(newTableName));
					sb.append("\n    " + colName + "  [" + oldColType + "->" + newColType + "]");
					columnChangeMap.put(newTableName, sb.toString());
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

	public static void main(String[] args) throws IOException {
		compareDiff("src/schema_v1", "src/schema_v2");
	}
}
