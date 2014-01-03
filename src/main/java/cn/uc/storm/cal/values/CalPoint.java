package cn.uc.storm.cal.values;

public class CalPoint {

	static public enum CalPointType {
		PV, UV
	}

	public final CalPointType type;
	public final String point;
	public final String uniqe;
	public final int count;
	public final long timtstamp;

	private CalPoint(CalPointType type, String point, String uniqe, int count,
			long timestamp) {
		this.type = type;
		this.point = point;
		this.uniqe = uniqe;
		this.count = count;
		this.timtstamp = timestamp;
	}
	
	@Override
	public String toString(){
		return type+","+point+","+uniqe+","+count+","+timtstamp;
	}

	static public CalPoint createPV(String point, int count) {
		return createPV(point, count, System.currentTimeMillis());
	}

	static public CalPoint createPV(String point, int count, long timestamp) {
		if (point == null) {
			return null;
		}
		return new CalPoint(CalPointType.PV, point, null, count, timestamp);
	}

	static public CalPoint createUV(String point, String uniqe, int count) {
		return createUV(point, uniqe, count, System.currentTimeMillis());
	}

	static public CalPoint createUV(String point, String uniqe, int count,
			long timestamp) {
		if (point == null || uniqe == null) {
			return null;
		}
		return new CalPoint(CalPointType.UV, point, uniqe, count, timestamp);
	}

}