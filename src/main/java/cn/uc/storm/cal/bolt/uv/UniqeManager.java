package cn.uc.storm.cal.bolt.uv;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import cn.uc.storm.cal.utils.Helper;
import cn.uc.storm.cal.values.UvKey;

/**
 * 保存管理多个uniqeSet 在添加uniqeSet的时候，需要注册一个过期时间。 UniqeManager会根据国企时间自动清理uniqeSet。
 * 
 * @author Administrator
 * 
 */

public class UniqeManager {
	static public Logger LOG = Logger.getLogger(UniqeManager.class);

	static public final String UNIQ_CLEAN_INTERVAL = "uc.storm.uniqe.clean.interval";
	static public final String UNIQESET_CLASS = "uc.storm.uniqeset.class";
	

	/**
	 * 用于获取最小有效期的UvKey TODO 使用最小堆实现
	 */
	private SortedMap<Long, UvKey> timeOutMap;
	// 生成序号，为了防止过期时间一样，过期时间统一*10000加上index
	private int index = 0;
	static private int offset = 20;
	static private int umask = (1 << 20) - 1;
	private Object lock = new Object();
	/**
	 * 日志超时时间
	 */
	private long logTimeOut;
	/**
	 * 保存所有的uniqeSet
	 */
	private Map<UvKey, UniqeSet> uniqeMap;

	// 清理线程
	private Timer timer;

	class CleanerTask extends TimerTask {
		@Override
		public void run() {
			synchronized(lock){
				LOG.info("into clean and report all");
				while( timeOutMap.size() > 0  ){
					long key = timeOutMap.firstKey();
					if( getValidityTime(key) < System.currentTimeMillis()){
						UvKey uvKey = timeOutMap.remove(key);
						UniqeSet uniqeSet = uniqeMap.remove(uvKey);
						LOG.info("remove uniqeSet:"+uvKey+" , "+uniqeSet);
					}
					else{
						break;
					}
				}
				LOG.info("there is "+uniqeMap.size()+" uniqe left");
				for(UniqeSet set:uniqeMap.values()){
					LOG.info(set);
				}
				
			}
		}
	}

	private Map conf;
	private Class uniqeSetClass;

	public UniqeManager(long logTimeOut, Map stormConf) {
		this.logTimeOut = logTimeOut;
		timeOutMap = new TreeMap<Long, UvKey>();
		uniqeMap = new HashMap<UvKey, UniqeSet>();
		conf = stormConf;
		//转化秒为毫秒
		long cleanInterval = ((Long) stormConf.get(UniqeManager.UNIQ_CLEAN_INTERVAL))*1000;
		// timer.schedule(arg0, arg1, arg2)
		CleanerTask task = new CleanerTask();
		timer = new Timer();
		timer.schedule(task, cleanInterval, cleanInterval);
		LOG.info("begin clean thread with cleanInterval: "+cleanInterval+" ms");
		try {
			uniqeSetClass = Helper.getClassFromConf(stormConf, UNIQESET_CLASS);
		} catch (Exception e) {
			LOG.error("get uniqe Set Class catch Exception", e);
		}
	}

	/**
	 * 判断某个Uvkey对应的指标是有含有这个新的uniqe
	 * 
	 * @param key
	 *            用于标记一个uv指标
	 * @param uniqe
	 *            用于记录去重的字符串(一般指uid)
	 * @return 如果是新的字符串则返回true，否则返回false
	 */
	public boolean isNew(UvKey key, String uniqe) {
		UniqeSet set = uniqeMap.get(key);
		if (set == null) {
			long validityTime = buildMapKey(key);
			set = newInstanceUniqeSet();
			timeOutMap.put(validityTime, key);

			uniqeMap.put(key, set);
		}
		return set.add(uniqe);
	}

	/**
	 * 通过uvkey计算这个uv指标的uniqeset的有效期 有效期 = ( 这个指标对应的时段 + 指标的周期长度 )+日志超时时间+ 容错间隔
	 * (设定为10秒)
	 * 
	 * @param key
	 * @return
	 */
	public long buildMapKey(UvKey key) {
		long temp = key.recordTs + key.calInterval + logTimeOut + 10000;
		return (temp << offset) + ((index++) & umask);
	}

	/**
	 * 通过mapkey获取有效期
	 */
	public long getValidityTime(long keyLong) {
		return keyLong >> offset;
	}

	/**
	 * 使用指定的类，创建一个UniqeSet
	 * 
	 * @return UniqeSet
	 */
	public UniqeSet newInstanceUniqeSet() {
		try {
			UniqeSet us = (UniqeSet) uniqeSetClass.newInstance();
			us.initFromConf(conf);
			return us;
		} catch (Exception e) {
			LOG.error("new uniqe Set with class+" + uniqeSetClass.getName()
					+ "+ catch Exception", e);
		}
		return null;
	}
	
	public void close(){
		timer.cancel();
	}
}
