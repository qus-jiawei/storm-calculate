package cn.uc.storm.cal.utils;

import org.junit.Test;

import junit.framework.TestCase;

public class TestPointCache extends TestCase {

	@Test
	public void testInc(){
		try{
			PointCache<Long,String> pc = new PointCache<Long,String>();
			pc.inc(0L, "a", 1);
			pc.inc(0L, "a", 1);
			while(pc.hasNext()){
				PointCache<Long,String>.ResultPoint temp = (PointCache<Long,String>.ResultPoint) pc.next();
				System.out.println(temp);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
