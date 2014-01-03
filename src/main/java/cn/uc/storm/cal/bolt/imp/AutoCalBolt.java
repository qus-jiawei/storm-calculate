package cn.uc.storm.cal.bolt.imp;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import backtype.storm.tuple.Tuple;
import cn.uc.storm.cal.bolt.CalBolt;
import cn.uc.storm.cal.utils.DateHelper;
import cn.uc.storm.cal.values.CalPoint;

public class AutoCalBolt extends CalBolt {

	@Override
	public List<CalPoint> getCalPoint(String logs) {
		StringTokenizer st = new StringTokenizer(logs,"*");
		List<CalPoint> calPointList = new ArrayList<CalPoint>();
		while (st.hasMoreTokens()) {
			String t = st.nextToken();
			String[] log = t.split(",");
			// 日志格式
			// 自动统计日志格式：4个字段,以逗号分隔,日志支持批量发送以*号分割
			if (log.length == 4) {
				CalPoint cp = getCalPoint(log);
				if( cp != null ){
					calPointList.add(cp);
				}
			}
		}
		return calPointList;
	}

	private CalPoint getCalPoint(String[] log) {
		try {
			if (log[1].length() == 0) {// PV
				return CalPoint.createPV(log[0], Integer.valueOf(log[2]),
						DateHelper.getTimestampFromDateTime(log[3]));

			} else {// UV
				return CalPoint.createUV(log[0], log[1],
						Integer.valueOf(log[2]),
						DateHelper.getTimestampFromDateTime(log[3]));

			}
		} catch (Exception e) {
			//do nothin as is normal that some log is worng format.
		}
		return null;
	}
}
