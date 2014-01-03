package cn.uc.storm.cal.bolt.uv;

import java.util.BitSet;
import java.util.Map;

import org.apache.log4j.Logger;

import cn.uc.storm.cal.utils.bloom.ByteBloomFilter;

/**
 * <pre>
 * Bloom filter 碰撞概率参考
 * http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
 * 
 * 内存计算使用量选择：假设用户总量N，共有A个UvBolt,则每个Bolt有N/A个用户需要存储。
 * 
 * 对于每个指标使用1M的内存来计算：
 * 为了使碰撞几率控制在1%左右，bit数组大小为
 * 
 * 
 * 
 * 
 * </pre>
 * @author Administrator
 *
 */
public class BloomUniqeSet implements UniqeSet {
	static public Logger LOG = Logger.getLogger(BloomUniqeSet.class);
	static public final String BYTE_SIZE = "uc.storm.uniqeset.bloom.bytesize";
	static public final String ERROR_RATE = "uc.storm.uniqeset.bloom.errorrate";
	
	private BitSet bits;
	/**
	 * Bloom filter的hash次数
	 */
	private int kFactor;
	/**
	 * 每个Bloom过滤器的数组大小
	 * TODO：根据不同指标的计算规模设定不同大小
	 */
	private long byteSize;
	private ByteBloomFilter bbf;
	
	private int checkByteSize(Object temp){
		if( temp instanceof Long ){
			return ((Long) temp).intValue();
		}
		else{//INTEGER
			return ((Integer)temp);
		}
	}
	
	@Override
	public void initFromConf(Map conf) {
		LOG.info(BYTE_SIZE);
		int byteSize = checkByteSize(conf.get(BYTE_SIZE));
		double errorRate = (Double) conf.get(ERROR_RATE);
		//申请的内存是1M的整数倍
		bbf = ByteBloomFilter.createBySize(byteSize, errorRate, 20);
		LOG.info(bbf.toString());
	}
	@Override
	public boolean add(String uniqe) {
		byte[] temp = uniqe.getBytes();
		if(bbf.contains(temp, 0, temp.length)){
			return true;
		}else{
			bbf.add(uniqe.getBytes());
			return false;
		}
	}

	@Override
	public void clean() {
		LOG.info(bbf.toString());
		bbf = null;
	}
	
	@Override
	public String toString(){
		return "BloomUniqeSet :"+bbf.toString();
	}

}
