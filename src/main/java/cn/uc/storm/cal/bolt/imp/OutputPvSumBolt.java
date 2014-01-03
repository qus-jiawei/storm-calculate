package cn.uc.storm.cal.bolt.imp;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uc.storm.cal.bolt.PvSumBolt;
import cn.uc.storm.cal.values.PvPoint;

public class OutputPvSumBolt extends PvSumBolt{
	static public Logger LOG = LoggerFactory.getLogger(OutputPvSumBolt.class);

	@Override
	public void flush(List<PvPoint> pvPointList) {
		for(PvPoint pvPoint:pvPointList){
			LOG.info(pvPoint.toString());
		}
		
	}

}
