package com.xl.study.json.parser;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class SchemaParser {

	public static void main(String[] args) throws IOException {
		File file = new File("src/global_mapping.json");
		File csv_file = new File("src/global_mapping.csv");
		
		System.out.println(file.getAbsolutePath());
		String jsonStr = FileUtils.readFileToString(file, "UTF-8");
		JSONObject jsonObject = JSON.parseObject(jsonStr);
		
		Map<String , Integer> statColumns = new HashMap<String , Integer>();
		Set<String> col_name_type = new HashSet<String>();
		List<String> lines = new ArrayList<String>();
		lines.add("tableName,columnType,columnName");
		
		JSONArray tables = jsonObject.getJSONArray("tables");
		for ( Object table : tables) {
			JSONObject tableJson = (JSONObject) table;
			
			String tableName = tableJson.getString("tableName");
			
			JSONArray columns = tableJson.getJSONArray("columns");
			for ( Object column : columns) {
				JSONObject columnJson = (JSONObject) column;
				
				String colName = columnJson.getString("name") ;
				String colType = columnJson.getString("type");
				
				//System.out.println(colName + " | " + colType);
				String line = tableName + "," + colType + "," + colName;
				lines.add(line);

				String name_type = colName + "_" + colType ;
				if(!col_name_type.contains(name_type)){
					
					col_name_type.add(name_type);
					
					if( statColumns.get(colType)==null ) {
						statColumns.put(colType, new Integer(0));
					}
					
					statColumns.put(colType, statColumns.get(colType) + 1);
					
					if( "DoubleType".equals(colType) ) {
						//System.out.println(colName + " | " + colType);
					}
				}
				
			}
			
		}
		System.out.println(statColumns);
		FileUtils.writeLines(csv_file, lines, false);
		//System.out.println(jsonObject);
		
	}

}
