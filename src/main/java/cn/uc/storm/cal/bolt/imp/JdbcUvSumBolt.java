package cn.uc.storm.cal.bolt.imp;

import java.sql.Statement;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import cn.uc.storm.cal.bolt.SubmitInit;
import cn.uc.storm.cal.bolt.UvSumBolt;
import cn.uc.storm.cal.utils.JdbcHelper;
import cn.uc.storm.cal.utils.MysqlHelper;
import cn.uc.storm.cal.values.UvPoint;

public class JdbcUvSumBolt extends UvSumBolt implements SubmitInit{
	
	static private final String UV_TABLE = "uc.storm.uv.table";
	private JdbcHelper jdbc;
	private String tableName;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		jdbc = new JdbcHelper(stormConf);
		tableName =  (String) stormConf.get(UV_TABLE);
		super.prepare(stormConf, context, collector);
	}
	
	@Override
	public void flush(List<UvPoint> uvPointList) {
		try {
			Statement stmt = jdbc.getStateMent();
			for(UvPoint uvPoint:uvPointList){
				String sql = MysqlHelper.getUpdateSqlFromUvPoint(tableName, uvPoint);
				boolean ret = stmt.execute(sql);
				LOG.info("flush the sql"+sql);
			}
			stmt.close();
			
		} catch (Exception e) {
			LOG.error("init mysql catch Exception", e);
		}
	}

	@Override
	public void cleanup() {
		jdbc.close();
	}

	@Override
	public void init(Map stormConf) {
		try {
			String tableName =  (String) stormConf.get(UV_TABLE);
			String createTableSql = MysqlHelper.getCreateUvTable(tableName);
			JdbcHelper.EasyExecute(stormConf, new String[]{createTableSql});
		} catch (Exception e) {
			LOG.error("init mysql catch Exception", e);
		}
	}
}
