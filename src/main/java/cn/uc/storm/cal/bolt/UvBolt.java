package cn.uc.storm.cal.bolt;

import java.util.HashMap;
import java.util.List;
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
import cn.uc.storm.cal.values.UvKey;
import cn.uc.storm.cal.values.UvValue;

/**
 * 
 * 计算uv的基本Bolt:
 * 和计算PV的最大的不同在于，需要对已经有的记录进行判断重复。
 * 
 * 
 * 两个核心时间：
 * 	计算周期: 数据最终汇总的时间周期。例如，计算周期是5分钟。1个小时就会产生12个指标。
 * 	推送时间: 每隔推送时间，将会推送新产生的数据到汇合Bolt。
 * 
 * 虚方法说明：
 * 	isnew是UvBolt判断某个需要去重的uid是不是新的uid的接口。
 *  
 *  通过继承UvBolt可以使用不同的方法提供去重的逻辑。当前提供UmUvBolt作为实现例子。
 * 
 * @author qiujw
 *
 */

public abstract class UvBolt extends BaseRichBolt {
	static public Logger LOG = Logger.getLogger(UvBolt.class);
	private Map<UvKey, UvValue> pointCache;
	
	/**
	 * 推送时间
	 */
	private long uvUpdateInterval;
	/**
	 * 计算周期
	 */
	private List<Long> uvCalInterval;
	/**
	 * 日志超时时间
	 */
	protected long logTimeOut;
	private int timeOutLog;
	private int logNumber;
	
	private Object lock;
	
	protected Timer timer;

	class EmitTimerTask extends TimerTask {
		private OutputCollector collector;

		public EmitTimerTask(OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void run() {
			Map<UvKey, UvValue> temp = null;
			synchronized (lock) {
				//为了减少锁定时间,使用引用置换方法
				temp = pointCache;
				pointCache = new HashMap<UvKey, UvValue>();
			}
			
			for(Entry<UvKey, UvValue> entry:temp.entrySet()){
				UvKey key = entry.getKey();
				UvValue value = entry.getValue();
				LOG.debug("uvbolt emit"+key+","+value);
				collector.emit(Constants.UV_STREAM_ID, new Values(
						key.calInterval,key.recordTs, key.point, 
						value.getConut()));
			}
			LOG.info("emit recive log:"+logNumber+" timeout log:"+timeOutLog+" emit log:"+temp.size());
			logNumber=0;
			timeOutLog=0;
		}
	};

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		uvUpdateInterval = (Long) stormConf.get(Constants.UV_UPDATE_INTERVAL);
		uvCalInterval = (List<Long>) stormConf.get(Constants.UV_CAL_INTERVAL);
		
		logTimeOut = (Long) stormConf.get(Constants.LOG_TIME_OUT);
				
		pointCache = new HashMap<UvKey, UvValue>();
		lock = new Object();
		// 设置TimerTask,每固定时间推送一次结果
		//第一次推送硬性设置为5秒，给予SumBolt有充足的初始化时间
		TimerTask timerTask = new EmitTimerTask(collector);
		timer = new Timer();
		timer.schedule(timerTask, Constants.FIRST_FLUSH_DELAY, uvUpdateInterval);
	}

	@Override
	public void execute(Tuple input) {
		String point = input.getString(0);
		String uniqe = input.getString(1);
		int count = input.getInteger(2);
		long ts = input.getLong(3);
		
		//可能有冲突,但影响不大不处理
		logNumber ++ ;
		
		if( System.currentTimeMillis() - ts > logTimeOut){
			//超时日志
			//直接抛弃不作处理
			//TODO 加入异步日志，打印收到的超时的日志总量 
			timeOutLog++;
			return ;
		}
		
		for(long calInterval:uvCalInterval){
			//对于每个计算周期都要计算一个去重的UV
			long recordTs = ts - (ts % calInterval);
			UvKey key = new UvKey(calInterval,recordTs,point);
			if( isNew(key,uniqe) ){
				synchronized (lock) {
					inc(key, count);
				}
			}
			
		}		
	}
	//计算去重的方法
	public abstract boolean isNew(UvKey key,String uniqe);
	
	private void inc(UvKey key,int count){
		UvValue value = pointCache.get(key);
		if( value == null ){
			value = new UvValue();
			pointCache.put(key, value);
		}
		value.inc(count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Constants.UV_STREAM_ID, new Fields("calInterval",
				"recordTs","point", "count"));
	}

	
}
