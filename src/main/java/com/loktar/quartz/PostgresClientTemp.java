//package com.loktar.quartz;
//
//
//import com.loktar.bean.Db;
//import com.loktar.bean.TxJar;
//import com.loktar.bean.TxQuartz;
//import org.apache.commons.dbcp2.BasicDataSource;
//import org.apache.commons.dbcp2.BasicDataSourceFactory;
//
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//public class PostgresClientTemp {
//	private Connection conn = null;
//
//	public PostgresClientTemp(Db db) {
//		Properties properties = new Properties();
//		properties.put("driverClassName", "org.postgresql.Driver");
//		properties.put("url", db.getUrl());
//		properties.put("username", db.getUsername());
//		properties.put("password", db.getPassword());
//		properties.put("initialSize", "5");
//		properties.put("maxActive", "100");
//		properties.put("maxIdle", "20");
//		properties.put("minIdle", "5");
//		properties.put("maxWait", "60000");
//		properties.put("connectionProperties", "true;characterEncoding=utf-8");
//		properties.put("defaultAutoCommit", "false");
//		try {
//			BasicDataSource ds = BasicDataSourceFactory.createDataSource(properties);
//			if (ds == null) {
//				ds = BasicDataSourceFactory.createDataSource(properties);
//			}
//			conn = ds.getConnection();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//	public List<TxQuartz> getTxQuartzs() throws SQLException {
//		Statement stmt = null;
//		List<TxQuartz> list = new ArrayList<>();
//		try {
//			stmt = conn.createStatement();
//			ResultSet rs = stmt.executeQuery("select * from txquartz where enable = 0");
//			while (rs.next()) {
//				list.add(new TxQuartz(rs.getInt(1), rs.getString(2), rs.getInt(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getInt(7)));
//			}
//			return list;
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			if (stmt != null)
//				stmt.close();
//			if (conn != null)
//				conn.close();
//		}
//		return list;
//	}
//
//	List<TxJar> getTxJars() throws SQLException {
//		Statement stmt = null;
//		List<TxJar> list = new ArrayList<>();
//		try {
//			stmt = conn.createStatement();
//			ResultSet rs = stmt.executeQuery("select * from txjar where enable = 0");
//			while (rs.next()) {
//				list.add(new TxJar(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getInt(5), rs.getString(6), rs.getString(7)));
//			}
//			return list;
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			if (stmt != null)
//				stmt.close();
//			if (conn != null)
//				conn.close();
//		}
//		return list;
//	}
//}
