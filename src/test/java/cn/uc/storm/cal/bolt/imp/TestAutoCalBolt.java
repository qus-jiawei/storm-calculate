package cn.uc.storm.cal.bolt.imp;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import cn.uc.storm.cal.values.CalPoint;

import backtype.storm.tuple.Values;

public class TestAutoCalBolt {

	@Test
	public void test() {
		AutoCalBolt acb = new AutoCalBolt();
		List<CalPoint> cpList = acb.getCalPoint("a,,5,20140102145300*b,u1,1,20140102145300*b,u2,1,20140102145300");
		for(CalPoint p: cpList){
			System.out.println(p);
		}
	}

}
