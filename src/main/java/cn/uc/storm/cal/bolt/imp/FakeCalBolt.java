package cn.uc.storm.cal.bolt.imp;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;
import cn.uc.storm.cal.bolt.CalBolt;
import cn.uc.storm.cal.values.CalPoint;

public class FakeCalBolt extends CalBolt{
	static public Logger LOG = LoggerFactory.getLogger(FakeCalBolt.class);
	private int sum = 0;
	@Override
	public List<CalPoint> getCalPoint(String logs) {
		List<CalPoint> temp = new ArrayList<CalPoint>();
		temp.add(CalPoint.createPV("a",1));
		temp.add(CalPoint.createPV("b",2));
		sum +=1;
		temp.add(CalPoint.createUV("c","uid1",1));
		temp.add(CalPoint.createUV("c","uid2",1));
		LOG.debug("emit log:" + sum);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return temp;
	}

}
