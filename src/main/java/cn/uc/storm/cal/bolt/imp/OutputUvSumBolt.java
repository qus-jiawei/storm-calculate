package cn.uc.storm.cal.bolt.imp;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uc.storm.cal.bolt.UvSumBolt;
import cn.uc.storm.cal.values.UvPoint;

public class OutputUvSumBolt extends UvSumBolt {
	static public Logger LOG = LoggerFactory.getLogger(OutputUvSumBolt.class);

	@Override
	public void flush(List<UvPoint> uvPointList) {
		for(UvPoint uvPoint:uvPointList){
			LOG.info(uvPoint.toString());
		}
	}
	
}
