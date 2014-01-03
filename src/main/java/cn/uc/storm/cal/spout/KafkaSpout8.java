package cn.uc.storm.cal.spout;

/**
 * 一个简单的连接kafka获取数据的spout,支持kafka0.8版本
 * 没有ack的容错保证。
 * 
 * @author qiujw
 *
 */
public class KafkaSpout8 {
//extends BaseRichSpout  {
//	private static Logger LOG = Logger.getLogger(KafKaSpout8.class);
//	public static final String KAFKA_TOPIC_KEY = "uc.storm.kafka.topic";
//	public static final String KAFKA_BATCH_NUMBER = "uc.storm.kafka.batch";
//	private SpoutOutputCollector collector;
//	private int batchNumber;
//	private ConsumerIterator<byte[], byte[]> it;
//
//	@Override
//	public void open(Map conf, TopologyContext context,
//			SpoutOutputCollector collector) {
//		try{
//			this.collector = collector;
//			batchNumber = (Integer) Helper.getOrDefault(conf,KAFKA_BATCH_NUMBER,10);
//			String topic = (String) conf.get(KAFKA_TOPIC_KEY);
//			it = getStreamFromTopic(conf,topic).iterator();
//			LOG.info("init finish");
//		}
//		catch(Exception e){
//			LOG.error("init kafka spout catch Exception",e);
//		}
//	}
//	private static ConsumerConfig createConsumerConfig(String zookeepers, String groupId) {
//		Properties props = new Properties();
//		props.put("zookeeper.connect", zookeepers);
//		props.put("group.id", groupId);
//		props.put("zookeeper.session.timeout.ms", "400");
//		props.put("zookeeper.sync.time.ms", "200");
//		props.put("auto.commit.interval.ms", "1000");
//		return new ConsumerConfig(props);
//	}
//	private KafkaStream getStreamFromTopic(Map conf,String topic){
//		List<String> zookeepers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
//		String zkStr = Helper.ListToString(zookeepers, ",");
//		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
//                createConsumerConfig(zkStr, "group"));
//		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//		//一个spout搭配一个kafka的stream
//        topicCountMap.put(topic, new Integer(1));
//        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
//        return streams.get(0);
//	}
//	@Override
//	public void nextTuple() {
//		for( int i=0; i<batchNumber && it.hasNext(); i++ ){
//			byte[] message = it.next().message();
//			collector.emit(new Values(new String(message)));
//		}
//	}
//
//	@Override
//	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("String"));
//	}
//
//	static public void main(String[] args){
//		System.out.println("weather kafka is ok? topic:"+args[0]);
//		
//		Config conf = new Config();
//		conf.put(Config.STORM_ZOOKEEPER_SERVERS, "hadoop3:2181,hadoop4:2181,hadoop5:2181");
//		KafKaSpout8 spout = new KafKaSpout8();
//		KafkaStream stream = spout.getStreamFromTopic(conf,args[0]);
//		while(true){
//			ConsumerIterator<byte[], byte[]> it = stream.iterator();
//			
//			while( it.hasNext() ){
//				System.out.println("get one");
//				byte[] message = it.next().message();
//				System.out.println(new String(message));
//			}
//			try{
//				Thread.sleep(1000);
//			}
//			catch(Exception e){
//				e.printStackTrace();
//			}
//		}
//	}
}
