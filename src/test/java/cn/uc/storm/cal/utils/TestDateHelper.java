package cn.uc.storm.cal.utils;

import static org.junit.Assert.*;

import java.text.ParseException;

import org.junit.Test;

public class TestDateHelper {

	@Test
	public void test() {
		try {
			long temp = DateHelper.getTimestampFromDateTime("20140103153700") ;
			long cal = 86400*1000;
			long recordTs = temp - (temp%cal);
			System.out.println( DateHelper.getDateTimeFromTs(recordTs)) ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
