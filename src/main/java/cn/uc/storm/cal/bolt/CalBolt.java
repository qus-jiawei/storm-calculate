package cn.uc.storm.cal.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.uc.storm.cal.main.Constants;
import cn.uc.storm.cal.values.CalPoint;

public abstract class CalBolt extends BaseRichBolt {
	static public Logger LOG = Logger.getLogger(CalBolt.class);
	private OutputCollector collector;
	@Override
    public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector){
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		String log = input.getString(0);
		List<CalPoint> cpl = getCalPoint(log);
		if (cpl != null) {
			for (CalPoint cp : cpl) {
				if(cp != null){
					switch (cp.type) {
					case PV:
						collector.emit(Constants.PV_STREAM_ID, new Values(cp.point,
								cp.count, cp.timtstamp));
						LOG.debug("emit to pv:"+cp.point+","+cp.count+","+cp.timtstamp);
						break;
					case UV:
						collector.emit(Constants.UV_STREAM_ID, new Values(cp.point,
								cp.uniqe, cp.count, cp.timtstamp));
						LOG.debug("emit to uv:"+cp.point+","+cp.uniqe+","+cp.count+","+cp.timtstamp);
						break;
					}
				}
			}
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Constants.PV_STREAM_ID, new Fields("point",
				"count", "timestamp"));
		declarer.declareStream(Constants.UV_STREAM_ID, new Fields("point",
				"uniqe", "count", "timestamp"));
	}

	public abstract List<CalPoint> getCalPoint(String logs);

}
