package cn.uc.storm.cal.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import backtype.storm.daemon.worker;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.uc.storm.cal.main.Constants;
import cn.uc.storm.cal.utils.PointCache;
import cn.uc.storm.cal.values.PvKey;
import cn.uc.storm.cal.values.PvPoint;
import cn.uc.storm.cal.values.PvValue;

/**
 * 
 * 汇总pv的Bolt（可看作计算所有累加结果的指标）:
 * 设立汇总Bolt的作用是为了减轻持久化层的负担。
 * 最终持久化层的负担可控制为=(pv指标数量*最长过期间隔数量)/更新时间
 * 
 * 两个核心时间：
 * 
 * 计算周期:数据最终汇总的时间周期。例如，计算周期是5分钟。1个小时就会产生12个指标。 
 * 推送时间: 每隔推送时间，将会调用持久化函数进行持久化。
 * 
 * 接收数据的格式： 
 * long recordTs（这个指标对应的时间单位)
 * String point(这个指标的名称) 
 * int count (这个指标新增加的数值) 
 * 
 * 处理逻辑:
 * 
 * 接收到的指标中间结果先在内存中进行汇总,每隔一段时间（pvUpdateInterval）进行持久化.
 * 
 * @author qiujw
 *
 */

public abstract class PvSumBolt extends BaseRichBolt  {
	static public Logger LOG = Logger.getLogger(PvBolt.class);
	private Map<PvKey, PvValue> pointCache;
	/**
	 * 推送时间
	 */
	private long pvUpdateInterval;
	/**
	 * 计算周期
	 */
	private long pvCalInterval;
	
	private Object lock;
	private Timer timer;
	//
	static private boolean flushing = false;
	
	class FlushTimerTask extends TimerTask {

		public FlushTimerTask() {
			
		}

		@Override
		public void run() {
			
			Map<PvKey, PvValue> temp = null;
			synchronized (lock) {
				//为了减少锁定时间,使用引用置换方法
				temp = pointCache;
				pointCache = new HashMap<PvKey, PvValue>();
			}
			List<PvPoint> pvPointList = new ArrayList<PvPoint>();
			for(Entry<PvKey, PvValue> entry:temp.entrySet()){
				PvKey key = entry.getKey();
				PvValue value = entry.getValue();
				LOG.debug("build pvPoint"+key+","+value);
				pvPointList.add(new PvPoint(key,value));
			}
			try{
				flush(pvPointList);
			}
			catch(Throwable e){
				LOG.error("PvSumBolt catch Exception while flush the pvpoint",e);
			}

		}
	};
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		pvUpdateInterval = (Long) stormConf.get(Constants.PV_UPDATE_INTERVAL);
		pvCalInterval = (Long) stormConf.get(Constants.PV_CAL_INTERVAL);
		
		pointCache = new HashMap<PvKey, PvValue>();
		lock = new Object();
		// 设置TimerTask,每固定时间推送一次结果
		TimerTask timerTask = new FlushTimerTask();
		timer = new Timer();
		timer.schedule(timerTask, pvUpdateInterval, pvUpdateInterval);
	}

	@Override
	public void execute(Tuple input) {
		long recordTs = input.getLong(0);
		String point = input.getString(1);
		int count = input.getInteger(2);
		synchronized (lock) {
			inc(recordTs, point, count);
		}
	}
	
	private void inc(long recordTs,String point,int count ){
		PvKey pvkey = new PvKey (recordTs,point);
		PvValue pvValue = pointCache.get(pvkey);
		if( pvValue ==null ){
			pvValue = new PvValue();
			pointCache.put(pvkey, pvValue);
		}
		pvValue.inc(count);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//nothing to emit
	}
	
	/**
	 * 业务开发者需要实现的持久化函数
	 * @param recordTs 指标对应的时间区间
	 * @param point 指标的名称
	 * @param count 指标是数值（累加值）
	 * 
	 */
	public abstract void flush(List<PvPoint> pvPointList);
	
	@Override
	public void cleanup() {
		timer.cancel();
	}  

}
