package cn.uc.storm.cal.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateHelper {
	static private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	static public String getDateTimeFromTs(long timestamp){
		return sdf.format(new Date(timestamp));
	}
	//将20131231010000格式转化为时间戳
	static public long getTimestampFromDateTime(String dateTime) throws ParseException{
		return sdf.parse(dateTime).getTime();
	}
}
