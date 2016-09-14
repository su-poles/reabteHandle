package com.poles.jsch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class DBUtil {

	// 通过ssh连接服务器主机，并返回会话Session
	public static Session openssh() {
		try {
			// 连接ssh
			JSch jsch = new JSch();
			Session session = jsch.getSession("read", "远程生产服务器ip地址", 22);
			session.setPassword("password");
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
//			System.out.println(session.getServerVersion());// 这里打印SSH服务器版本信息

			// 通过ssh登录182.92.102.64，并read用户去连接数据库：localhost去连接数据库
			// ssh -L localhost:3306:xxxx.mysql.rds.aliyuncs.com:3306
			// read@182.92.102.64 正向代理
			session.setPortForwardingL(3306, "xxx.mysql.rds.aliyuncs.com", 3306);
			return session;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Connection getConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/duomeidai", "readonly",
					"password");
			return conn;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void disConnection(Session session) {
		if (session != null) {
			session.disconnect();
		}
	}

	public static void close(Connection conn, Statement stmt) {
		close(conn, stmt, null);
	}
	
	public static void close(Statement stmt, ResultSet rs) {
		close(null, stmt, rs);
	}
	
	public static void close(Connection conn) {
		close(conn, null, null);
	}
	
	public static void close(Connection conn, Statement stmt, ResultSet rs) {
		try {
			if (rs != null)rs.close();
			if (stmt != null)stmt.close();
			if (conn != null)conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
