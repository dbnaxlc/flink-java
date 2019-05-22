package com.xzq.flink.db.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectionUtil {
	
	private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";

	public static Connection getConnection() throws Exception {
		Class.forName(DRIVER);
		Connection connection = DriverManager.getConnection(
				"jdbc:oracle:thin:@10.37.30.100:1521:kftdb", "kftbank2",
				"Bnk102%");
		connection.setAutoCommit(false);
		return connection;
	}

	public static void releaseConnection(Connection conn) {
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}

	public static Statement getStatement(Connection conn) throws SQLException {
		return conn.createStatement();
	}

	public static ResultSet getResultSet(Statement statement, String sql)
			throws SQLException {
		return statement.executeQuery(sql);
	}
}
