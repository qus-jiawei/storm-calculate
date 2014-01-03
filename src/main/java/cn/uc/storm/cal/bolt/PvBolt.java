package cn.uc.storm.cal.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.uc.storm.cal.main.Constants;
import cn.uc.storm.cal.utils.PointCache;
import cn.uc.storm.cal.values.PvKey;
import cn.uc.storm.cal.values.PvValue;

/**
 * 
 * 计算pv的基本Bolt（可看作计算所有累加结果的指标） 
 * 两个核心时间： 
 * 计算周期:数据最终汇总的时间周期。例如，计算周期是5分钟。1个小时就会产生12个指标。
 * 推送时间: 每隔推送时间，将会推送新产生的数据到汇合Bolt。
 * 
 * 接收数据的格式： 
 * String point(这个指标的名称) 
 * int count (这个指标的要累加的数值) 
 * long timestamp(这个日志的产生时间)
 * 
 * 推送数据的格式: 
 * 
 * long recordTs（这个指标对应的时间单位)：一定是pvCalInterval的整数倍
 * String point(这个指标的名称) 
 * int count (这个指标新增加的数值) 
 * 
 * 处理逻辑:
 * 
 * 接收到的日志现在内存中进行汇总,每隔一段时间（pvUpdateInterval）推送到后面的汇总的Bolt.
 * 
 * @author qiujw
 * 
 */
public class PvBolt extends BaseRichBolt {
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
	/**
	 * 日志超时时间
	 */
	private long logTimeOut;
	
	private Object lock;
	
	private Timer timer;

	class EmitTimerTask extends TimerTask {
		private OutputCollector collector;

		public EmitTimerTask(OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void run() {
			Map<PvKey, PvValue> temp = null;
			synchronized (lock) {
				//为了减少锁定时间,使用引用置换方法
				temp = pointCache;
				pointCache = new HashMap<PvKey, PvValue>();
			}
			
			for(Entry<PvKey, PvValue> entry:temp.entrySet()){
				PvKey key = entry.getKey();
				PvValue value = entry.getValue();
				LOG.debug("pvbolt emit"+key+","+value);
				collector.emit(Constants.PV_STREAM_ID, new Values(
						key.recordTs, key.point, value.getConut()));
			}

		}
	};

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		pvUpdateInterval = (Long) stormConf.get(Constants.PV_UPDATE_INTERVAL);
		pvCalInterval = (Long) stormConf.get(Constants.PV_CAL_INTERVAL);
		
		logTimeOut = (Long) stormConf.get(Constants.LOG_TIME_OUT);
		
		pointCache = new HashMap<PvKey, PvValue>();
		lock = new Object();
		// 设置TimerTask,每固定时间推送一次结果
		TimerTask timerTask = new EmitTimerTask(collector);
		timer = new Timer();
		timer.schedule(timerTask, Constants.FIRST_FLUSH_DELAY, pvUpdateInterval);
	}

	// point,count,timestamp
	@Override
	public void execute(Tuple input) {
		String point = input.getString(0);
		int count = input.getInteger(1);
		long ts = input.getLong(2);
		
		if( System.currentTimeMillis() - ts > logTimeOut){
			//超时日志
			//直接抛弃不作处理
			//TODO 加入异步日志，打印收到的超时的日志总量 
			return ;
		}

		long recordTs = ts - (ts % pvCalInterval);
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
		declarer.declareStream(Constants.PV_STREAM_ID, new Fields("recordTs",
				"point", "count"));
	}
	
	@Override
	public void cleanup(){
		if( timer!=null) {
			timer.cancel();
		}
	}

}
