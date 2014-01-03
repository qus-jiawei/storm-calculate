package cn.uc.storm.cal.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.uc.storm.cal.utils.Helper;

public class KafkaSpout7 extends BaseRichSpout  {
	private static Logger LOG = Logger.getLogger(KafkaSpout7.class);
	public static final String KAFKA_TOPIC_KEY = "uc.storm.kafka.topic";
	public static final String KAFKA_BATCH_NUMBER = "uc.storm.kafka.batch";
	private SpoutOutputCollector collector;
	private int batchNumber;
	private ConsumerIterator<Message> it;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try{
			this.collector = collector;
			batchNumber = ((Long) Helper.getOrDefault(conf,KAFKA_BATCH_NUMBER,10L) ).intValue();
			String topic = (String) conf.get(KAFKA_TOPIC_KEY);
			it = getStreamFromTopic(conf,topic).iterator();
			LOG.info("init finish");
		}
		catch(Exception e){
			LOG.error("init kafka spout catch Exception",e);
		}
	}
	private static ConsumerConfig createConsumerConfig(String zookeepers, String groupId) {
		Properties props = new Properties();
		//for kafka7
		props.put("zk.connect", zookeepers);
		props.put("groupid", groupId);
		props.put("zk.session.timeout.ms", "400");
		props.put("zk.sync.time.ms", "200");
		props.put("autocommit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}
	@SuppressWarnings("unchecked")
	private KafkaStream<Message> getStreamFromTopic(Map conf,String topic){
		List<String> zookeepers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
		String zkStr = Helper.ListToString(zookeepers, ",");
		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zkStr, "group"));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		//一个spout搭配一个kafka的stream
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<Message>> streams = consumerMap.get(topic);
        return streams.get(0);
	}
	@Override
	public void nextTuple() {
		for( int i=0; i<batchNumber && it.hasNext(); i++ ){
			Message message = it.next().message();
			String temp = Helper.getMessage(message.payload());
			collector.emit(new Values(temp));
			LOG.debug("emit log:"+temp);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("String"));
	}

	static public void main(String[] args){
		System.out.println("weather kafka is ok? topic:"+args[0]);
		
		Config conf = new Config();
		List<String> zkserver = new ArrayList<String>();
		zkserver.add("hadoop3:2181");
		zkserver.add("hadoop4:2181"); 
		zkserver.add("hadoop5:2181");
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, zkserver);
		KafkaSpout7 spout = new KafkaSpout7();
		KafkaStream stream = spout.getStreamFromTopic(conf,args[0]);
		while(true){
			ConsumerIterator<Message> it = stream.iterator();
			
			while( it.hasNext() ){
				System.out.println("get one");
				String temp = Helper.getMessage(it.next().message().payload());
				System.out.println(temp);
			}
			try{
				Thread.sleep(1000);
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
