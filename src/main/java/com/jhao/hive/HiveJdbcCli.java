package com.jhao.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class HiveJdbcCli {


	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://localhost:10000/default";
	private static String user = "root";
	private static String password = "123456";
	private static String sql = "";
	private static ResultSet res;
	private static final Logger log = Logger.getLogger(HiveJdbcCli.class);

	public static void main(String[] args) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConn();
			stmt = conn.createStatement();
			createTable(stmt, "person");
			showTables(stmt, "person");
			System.out.println("-------------next----------------");
			System.out.println();
			System.out.println();
			// 执行describe table操作
			 describeTables(stmt, "person");
			 System.out.println("-------------next----------------");
			 System.out.println();
			 System.out.println();
			// 执行load data into table操作
			 loadData(stmt, "person");
			 System.out.println("-------------next----------------");
			 System.out.println();
			System.out.println();
			// 执行 select * query 操作
			 selectData(stmt, "person");
			 System.out.println("-------------next----------------");
			 System.out.println();
			System.out.println();
			// 执行 regular hive query 统计操作
			 countData(stmt, "person");


		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.error(driverName + " not found!", e);
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
			log.error("Connection error!", e);
			System.exit(1);
		} finally {
			try {
				if (conn != null) {
					conn.close();
					conn = null;
				}
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static void countData(Statement stmt, String tableName)
			throws SQLException {
		sql = "select count(1) from " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行“regular hive query”运行结果:");
		while (res.next()) {
			System.out.println("count ------>" + res.getString(1));
		}
	}

	private static void selectData(Statement stmt, String tableName)
			throws SQLException {
		sql = "select * from " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行 select * query 运行结果:");
		while (res.next()) {
			System.out.println(res.getInt(1) + "\t" + res.getString(2));
		}
	}

	private static void loadData(Statement stmt, String tableName)
			throws SQLException {
		String filepath = "/opt/people.txt";
		sql = "load data local inpath '" + filepath + "' into table "
				+ tableName;
		System.out.println("Running:" + sql);
		stmt.execute(sql);
	}

	private static void describeTables(Statement stmt, String tableName)
			throws SQLException {
		sql = "describe " + tableName;
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行 describe table 运行结果:");
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
	}

	private static void showTables(Statement stmt, String tableName)
			throws SQLException {
		sql = "show tables '" + tableName + "'";
		System.out.println("Running:" + sql);
		res = stmt.executeQuery(sql);
		System.out.println("执行 show tables 运行结果:");
		if (res.next()) {
			System.out.println(res.getString(1));
		}
	}

	private static void createTable(Statement stmt, String tableName)
			throws SQLException {
		sql = "create table "
				+ tableName
				+ " (id int, sex string, name string)  row format delimited fields terminated by ','";
		stmt.execute(sql);
	}

	private static String dropTable(Statement stmt) throws SQLException {
		// 创建的表名
		String tableName = "testHive";
		sql = "drop table " + tableName;
		stmt.executeQuery(sql);
		return tableName;
	}

	private static Connection getConn() throws ClassNotFoundException,
			SQLException {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}
}
