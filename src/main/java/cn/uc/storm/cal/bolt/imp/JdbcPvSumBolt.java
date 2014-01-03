package cn.uc.storm.cal.bolt.imp;

import java.sql.Statement;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import cn.uc.storm.cal.bolt.PvSumBolt;
import cn.uc.storm.cal.bolt.SubmitInit;
import cn.uc.storm.cal.utils.JdbcHelper;
import cn.uc.storm.cal.utils.MysqlHelper;
import cn.uc.storm.cal.values.PvPoint;

public class JdbcPvSumBolt extends PvSumBolt implements SubmitInit {
	
	static private final String PV_TABLE = "uc.storm.pv.table";
	private JdbcHelper jdbc;
	private String tableName;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		jdbc = new JdbcHelper(stormConf);
		tableName =  (String) stormConf.get(PV_TABLE);
		super.prepare(stormConf, context, collector);
	}
	
	@Override
	public void flush(List<PvPoint> pvPointList) {
		try {
			Statement stmt = jdbc.getStateMent();
			for(PvPoint pvPoint:pvPointList){
				String sql = MysqlHelper.getUpdateSqlFromPvPoint(tableName,pvPoint);
				boolean ret = stmt.execute(sql);
				LOG.debug("flush the sql"+sql);
			}
			stmt.close();
			
		} catch (Exception e) {
			LOG.error("flushing mysql catch Exception", e);
		}
	}

	@Override
	public void cleanup() {
		jdbc.close();
	}

	@Override
	public void init(Map stormConf) {
		try {
			String tableName =  (String) stormConf.get(PV_TABLE);
			String createTableSql = MysqlHelper.getCreatePvTable(tableName);
			JdbcHelper.EasyExecute(stormConf, new String[]{createTableSql});
		} catch (Exception e) {
			LOG.error("init mysql catch Exception", e);
		}
	}
}
