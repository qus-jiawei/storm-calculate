package cn.uc.storm.cal.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cn.uc.storm.cal.values.UvKey;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

/** 最简单的UvBolt实现：
 * 		不提供定时清理服务，仅用于测试之用
 * 
 */
public class SimpleUvBolt extends UvBolt{

	private Map<UvKey, Set<String>> uidSet;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector){
		super.prepare(stormConf,context,collector);
		uidSet = new HashMap<UvKey, Set<String>>();
	}
	
	@Override
	public boolean isNew(UvKey key, String uniqe) {
		// TODO Auto-generated method stub
		Set<String> set = uidSet.get(key);
		if(set==null){
			set = new TreeSet<String>();
			uidSet.put(key, set);
		}
		return set.add(uniqe);
	}

}
