package cn.uc.storm.cal.bolt;

import java.util.ArrayList;
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
import backtype.storm.tuple.Tuple;
import cn.uc.storm.cal.main.Constants;
import cn.uc.storm.cal.values.UvKey;
import cn.uc.storm.cal.values.UvPoint;
import cn.uc.storm.cal.values.UvValue;

public abstract class UvSumBolt extends BaseRichBolt  {
	
	static public Logger LOG = Logger.getLogger(UvSumBolt.class);
	
	private Map<UvKey, UvValue> pointCache;
	/**
	 * 推送时间
	 */
	private long uvUpdateInterval;
	/**
	 * 计算周期
	 */
	private List<Long> uvCalInterval;
	
	private Object lock;
	private Timer timer;
	//
	static private boolean flushing = false;
	
	class FlushTimerTask extends TimerTask {

		public FlushTimerTask() {
			
		}

		@Override
		public void run() {
			
			Map<UvKey, UvValue> temp = null;
			synchronized (lock) {
				//为了减少锁定时间,使用引用置换方法
				temp = pointCache;
				pointCache = new HashMap<UvKey, UvValue>();
			}
			List<UvPoint> uvPointList = new ArrayList<UvPoint>();
			for(Entry<UvKey, UvValue> entry:temp.entrySet()){
				UvKey key = entry.getKey();
				UvValue value = entry.getValue();
				LOG.debug("build uvPoint"+key+","+value);
				uvPointList.add(new UvPoint(key,value));
			}
			try{
				flush(uvPointList);
			}
			catch(Throwable e){
				LOG.error("UvSumBolt catch Exception while flush the uvpoint",e);
			}

		}
	};
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		uvUpdateInterval = (Long) stormConf.get(Constants.UV_UPDATE_INTERVAL);
		uvCalInterval = (List<Long>) stormConf.get(Constants.UV_CAL_INTERVAL);
		
		pointCache = new HashMap<UvKey, UvValue>();
		lock = new Object();
		// 设置TimerTask,每固定时间推送一次结果
		TimerTask timerTask = new FlushTimerTask();
		timer = new Timer();
		timer.schedule(timerTask, Constants.FIRST_FLUSH_DELAY, uvUpdateInterval);
		
	}

	@Override
	public void execute(Tuple input) {
		long calInterval = input.getLong(0);
		long recordTs = input.getLong(1);
		String point = input.getString(2);
		int count = input.getInteger(3);
		UvKey uvkey = new UvKey (calInterval,recordTs,point);
		synchronized (lock) {
			inc(uvkey, count);
		}
	}
	
	private void inc(UvKey uvkey,int count ){
		UvValue uvValue = pointCache.get(uvkey);
		if( uvValue ==null ){
			uvValue = new UvValue();
			pointCache.put(uvkey, uvValue);
		}
		uvValue.inc(count);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//nothing to emit
	}
	
	public abstract void flush(List<UvPoint> uvPointList);
	
	@Override
	public void cleanup() {
		timer.cancel();
	}  
}
