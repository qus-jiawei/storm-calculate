package cn.uc.storm.cal.spout;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.daemon.worker;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MemSpout extends BaseRichSpout {
	static public Logger LOG = Logger.getLogger(MemSpout.class);
	private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		collector.emit(new Values("haha"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {
		
	}
}
