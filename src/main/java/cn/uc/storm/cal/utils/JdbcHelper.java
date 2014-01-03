package cn.uc.storm.cal.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;

public class JdbcHelper {
	static public Logger LOG = Logger.getLogger(JdbcHelper.class);

	static private final String JDBC_CONNECTION = "uc.storm.jdbc.connect";
	static private final String JDBC_DRIVER = "uc.storm.jdbc.driver";
	static private final String JDBC_USER = "uc.storm.jdbc.user";
	static private final String JDBC_PASSWD = "uc.storm.jdbc.passwd";

	private String jdbcConnectionString;
	private String jdbcDriver;
	private String jdbcUser;
	private String jdbcPasswd;
	private Connection conn;

	public JdbcHelper(Map stormConf) {
		jdbcConnectionString = (String) stormConf.get(JDBC_CONNECTION);
		jdbcDriver = (String) stormConf.get(JDBC_DRIVER);
		jdbcUser = (String) stormConf.get(JDBC_USER);
		jdbcPasswd = (String) stormConf.get(JDBC_PASSWD);
		initConnection();
	}

	private void initConnection() {
		try {
			// 加载Connector/J驱动
			Class.forName(jdbcDriver);
			// 建立连接
			conn = DriverManager.getConnection(jdbcConnectionString, jdbcUser,
					jdbcPasswd);
		} catch (Exception e) {
			LOG.error("init mysql catch Exception", e);
		}
	}
	
	public Statement getStateMent() {
		try {
			return conn.createStatement();
		} catch (SQLException e) {
			LOG.error("create statement catch Exception",e);
		}
		return null;
	}

	public void close() {
		if( conn!= null){
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.error("close jdbc catch Exception",e);
			}
		}
	}
	static public void EasyExecute(Map stormConf,String[] sqls) throws SQLException{
		JdbcHelper jdbc = new JdbcHelper(stormConf);
		Statement state = jdbc.getStateMent();
		for(String sql:sqls){
			state.execute(sql);
		}
		jdbc.close();
	}
}
