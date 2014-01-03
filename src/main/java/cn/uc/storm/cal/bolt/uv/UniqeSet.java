package cn.uc.storm.cal.bolt.uv;

import java.util.Map;


/**
 * 针对不同需要去重的指标，提供一个存储管理去重key的类。
 * 
 * 每个Uv指标都对应一个需要去重的uid集合。
 * 每个不同的Uv指标都可以使用UvKey来区分，对应的UniqeSet就是这个指标对应的uid的去重集合。
 * 
 * 接口:
 * 
 * 
 * @author qiujw
 *
 */
public interface UniqeSet {
	/**
	 * 设置conf
	 * @param conf
	 */
	public void initFromConf(Map conf);
	/**
	 * 添加一个去重的uniqe
	 * @param uniqe 去重的string
	 * @return 是否是新的key
	 */
	public boolean isNewAndadd(String uniqe);
	/**
	 * 进行清理
	 * 如果有外部资源依赖可以在这里关闭
	 */
	public void clean();
}
