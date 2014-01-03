package cn.uc.storm.cal.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateHelper {
	static private ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>(){
		@Override 
		protected SimpleDateFormat initialValue() {
			SimpleDateFormat temp = new SimpleDateFormat("yyyyMMddHHmmss");
			temp.setTimeZone(TimeZone.getTimeZone("GMT+0:00"));
			return temp;
		}
	};
	
	static public String getDateTimeFromTs(long timestamp){
		return sdf.get().format(new Date(timestamp));
	}
	//将20131231010000格式转化为时间戳
	static public long getTimestampFromDateTime(String dateTime) throws ParseException{
		return sdf.get().parse(dateTime).getTime();
	}
}
