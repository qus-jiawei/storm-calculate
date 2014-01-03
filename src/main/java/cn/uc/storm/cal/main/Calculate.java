package cn.uc.storm.cal.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import cn.uc.storm.cal.bolt.CalBolt;
import cn.uc.storm.cal.bolt.PvBolt;
import cn.uc.storm.cal.bolt.PvSumBolt;
import cn.uc.storm.cal.bolt.SubmitInit;
import cn.uc.storm.cal.bolt.UvBolt;
import cn.uc.storm.cal.bolt.UvSumBolt;
import cn.uc.storm.cal.utils.Helper;

/**
 * <pre>
 * 构建实时计算指标的基础类 数据流的流动过程：
 *  spout->业务计算的Bolt->PvBolt->PvSumBolt 
 *  		|
 *  		->UvBolt->UvSumBolt
 * 
 * PvBolt用于计算可直接累加的数据,采用随机订阅方式 PvSumBolt用于汇总PvBolt的数据,采用按字段哈希订阅方式,推送到持久化层
 * 
 * UvBolt用于计算需要通过制定的uid去重的数据，采用按字段哈希的订阅方式
 * UvSumBolt用于汇总UvBolt的数据,采用按字段哈希的订阅方式,推送到持久化层
 * </pre>
 * @author qiujw
 * 
 * TODO：将时间改为int，节省内存。
 */
public class Calculate {
	static public Logger LOG = Logger.getLogger(Calculate.class);
	
	private List<SubmitInit> sumbitInitList = null;
	
	public Calculate() {

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void sumbit(Map conf) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		try {
			TopologyBuilder builder = new TopologyBuilder();
			
			//设置spout
			IRichSpout spout = (IRichSpout) Helper.newInstanceFromConf(conf ,Constants.CAL_SPOUT);
			int spoutNum = (Integer) conf.get(Constants.CAL_SPOUT_NUM);
			builder.setSpout(Constants.SPOUT_ID, spout, spoutNum);
			addSubmitInit(spout);
			
			//设置CalBolt,使用local加速
			CalBolt calBolt = (CalBolt) Helper.newInstanceFromConf(conf ,Constants.CAL_CALBOLT);
			int calBoltNum = (Integer) conf.get(Constants.CAL_CALBOLT_NUM);
			builder.setBolt(Constants.CAL_BOLT_ID, calBolt, calBoltNum)
					.localOrShuffleGrouping(Constants.SPOUT_ID);
			addSubmitInit(calBolt);
			
			//添加PV流
			PvBolt pvBolt = (PvBolt) Helper.newInstanceFromConf(conf ,Constants.CAL_PVBOLT);
			int pvBoltNum = (Integer) conf.get(Constants.CAL_PVBOLT_NUM);
			
			PvSumBolt pvSumBolt = (PvSumBolt) Helper.newInstanceFromConf(conf ,Constants.CAL_PVSUMBOLT);
			int pvSumBoltNum = (Integer) conf.get(Constants.CAL_PVSUMBOLT_NUM);
			
			builder.setBolt(Constants.PV_BOLT_ID, pvBolt, pvBoltNum)
					.shuffleGrouping(Constants.CAL_BOLT_ID,Constants.PV_STREAM_ID);
			builder.setBolt(Constants.PV_SUM_BOLT_ID, pvSumBolt, pvSumBoltNum)
					.fieldsGrouping(Constants.PV_BOLT_ID, Constants.PV_STREAM_ID,
							new Fields("point"));
			addSubmitInit(pvBolt);
			addSubmitInit(pvSumBolt);
			
			//添加uv流
			UvBolt uvBolt = (UvBolt) Helper.newInstanceFromConf(conf ,Constants.CAL_UVBOLT);
			int uvBoltNum = (Integer) conf.get(Constants.CAL_UVBOLT_NUM);
			
			UvSumBolt uvSumBolt = (UvSumBolt) Helper.newInstanceFromConf(conf ,Constants.CAL_UVSUMBOLT);
			int uvSumBoltNum = (Integer) conf.get(Constants.CAL_UVSUMBOLT_NUM);
			
			
			builder.setBolt(Constants.UV_BOLT_ID, uvBolt, uvBoltNum)
					.fieldsGrouping(Constants.CAL_BOLT_ID, Constants.UV_STREAM_ID,
							new Fields("uniqe"));
			builder.setBolt(Constants.UV_SUM_BOLT_ID, uvSumBolt, uvSumBoltNum)
					.fieldsGrouping(Constants.UV_BOLT_ID, Constants.UV_STREAM_ID,
							new Fields("point"));
			
			addSubmitInit(uvBolt);
			addSubmitInit(uvSumBolt);
			
			Utils.isValidConf(conf);
			
			// 运行一些Bolt的初始化钩子
			SubmitInitAll(conf);
			
			StormTopology topology = builder.createTopology();
			String topologyId = (String) conf.get(Constants.TOPOLOGY_ID);
			String local = (String) conf.get(Constants.LOCAL_MODE);
			if ( local != null ) {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyId, conf, topology);
				Utils.sleep(10000);
			} else {
				StormSubmitter.submitTopology(topologyId, conf, topology);
			}
		} catch (Exception e) {
			LOG.error("sumbit catch Exception",e);
		}
	}
	
	private void addSubmitInit(Object o){
		if( o instanceof SubmitInit){
			if( sumbitInitList == null){
				sumbitInitList = new ArrayList<SubmitInit>();
			}
			sumbitInitList.add((SubmitInit) o );
		}
	}
	@SuppressWarnings("rawtypes")
	private void SubmitInitAll(Map conf){
		if( sumbitInitList!=null){
			for(SubmitInit si:sumbitInitList){
				si.init(conf);
			}
		}
	}

	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static public void main(String[] args){
		try{
			String fileName = null;
			if( args.length == 0 ){
				System.out.println("usage: config_file [TopologyName] ");
			}
			fileName = args[0];
			Map conf = Utils.findAndReadConfigFile("storm-cal-default.yaml", true);
			Map temp = Helper.readConfigYaml(fileName);
			conf.putAll(temp);
			System.out.println("file is:" + fileName);
			System.out.println("config is:" + conf.toString());
			String topologyId = null;
			if( args.length >= 2 ){
				topologyId = args[1];
			}
			else{
				topologyId = Long.toString( System.currentTimeMillis() );
			}
			Helper.ifNullAndSet(conf , Constants.TOPOLOGY_ID, topologyId);
			Calculate cal = new Calculate();
			//为方便使用,配置文件的时间单位都是秒数
			//这里统一将相关的配置转化为毫秒
			String[] changeList = new String[]{
					Constants.PV_UPDATE_INTERVAL,	
					Constants.PV_CAL_INTERVAL,
					Constants.UV_UPDATE_INTERVAL,	
					Constants.UV_CAL_INTERVAL,
					Constants.LOG_TIME_OUT
			};
			toMillSecond(conf,changeList);
			
			cal.sumbit(conf);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private static void toMillSecond(Map conf, String[] changeList) {
		for(String key:changeList){
			Object temp = conf.get(key);
			if( temp instanceof Integer){
				int time = (Integer) conf.get(key);
				long newTime = time * 1000;
				conf.put(key, newTime);
			}
			else if( temp instanceof List){
				List<Integer> timeList = (List<Integer>) temp;
				List<Long> newTimeList = new ArrayList<Long>();
				for(Integer i:timeList){
					newTimeList.add(new Long(i*1000));
				}
				conf.put(key, newTimeList);
			}
		}
		
	}
}
