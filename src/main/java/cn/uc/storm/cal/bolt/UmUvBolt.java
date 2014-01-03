package cn.uc.storm.cal.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import cn.uc.storm.cal.bolt.uv.UniqeManager;
import cn.uc.storm.cal.values.UvKey;

/**
 * UvBolt的一个实现：
 * 	使用UniqeManager保存和管理所有的uniqe key.
 *  
 * 
 * @author qiujw
 *
 */
public class UmUvBolt extends UvBolt{
	
	private UniqeManager um;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		um = new UniqeManager(this.timer,this.logTimeOut,stormConf);
	}

	@Override
	public boolean isNew(UvKey key, String uniqe) {
		return um.isNew(key, uniqe);
	}
	
	@Override
	public void cleanup() {
		timer.cancel();
	}  
}
