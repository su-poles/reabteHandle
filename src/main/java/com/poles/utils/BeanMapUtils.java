package com.poles.utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jcraft.jsch.Session;
import com.poles.jsch.DBUtil;

public class BeanMapUtils {
	public static List<Map<String, String>> querySQLToMap(String querySQL){
		if(querySQL == null || !(querySQL.trim().toLowerCase().startsWith("select"))){
			return new ArrayList<Map<String, String>>();
		}
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Session session = null;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
        	session = DBUtil.openssh();
        	conn = DBUtil.getConnection();
        	stmt = conn.createStatement();
        	rs = stmt.executeQuery(querySQL);
        	// 获取列名  
            ResultSetMetaData metaData = rs.getMetaData();  
            while(rs.next()){
            	Map<String, String> map = new HashMap<String, String>();
            	for (int i = 0; i < metaData.getColumnCount(); i++) {
            		// rs数据下标从1开始  
            		String columnName = metaData.getColumnName(i + 1);  
            		int type = metaData.getColumnType(i + 1);  
            		if( Types.DATE == type){
            			map.put(columnName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getDate(columnName)));
            		} else{
            			map.put(columnName, rs.getString(columnName));
            		}
            	}
            	list.add(map);
            }
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			DBUtil.close(conn, stmt, rs);
			DBUtil.disConnection(session);
		}
        return list;
	}

	public static BigDecimal queryReabte(String queryTotale) {
		List<Map<String, String>> list = querySQLToMap(queryTotale);
		Map<String, String> map = list.get(0);
		String[] values = map.values().toArray(new String[0]);
		return new BigDecimal(values[0]);
	}
	
	public static BigDecimal queryReabte(Connection conn, String queryTotale) {
		List<Map<String, String>> list = querySQLToMap(conn, queryTotale);
		Map<String, String> map = list.get(0);
		String[] values = map.values().toArray(new String[0]);
		return new BigDecimal(values[0]);
	}
	
	
	public static List<Map<String, String>> querySQLToMap(Connection conn, String querySQL){
		if(querySQL == null || !(querySQL.trim().toLowerCase().startsWith("select"))){
			return new ArrayList<Map<String, String>>();
		}
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
        	stmt = conn.createStatement();
        	rs = stmt.executeQuery(querySQL);
        	// 获取列名  
            ResultSetMetaData metaData = rs.getMetaData();  
            while(rs.next()){
            	Map<String, String> map = new HashMap<String, String>();
            	for (int i = 0; i < metaData.getColumnCount(); i++) {
            		// rs数据下标从1开始  
            		String columnName = metaData.getColumnName(i + 1);  
            		int type = metaData.getColumnType(i + 1);  
            		if( Types.DATE == type){
            			map.put(columnName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rs.getDate(columnName)));
            		} else{
            			map.put(columnName, rs.getString(columnName));
            		}
            	}
            	list.add(map);
            }
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			DBUtil.close(stmt, rs);
		}
        return list;
	}
}
