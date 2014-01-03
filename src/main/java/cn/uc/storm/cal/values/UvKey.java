package cn.uc.storm.cal.values;

public class UvKey {

	public final long calInterval;
	public final long recordTs;
	public final String point;

	public UvKey(long calInterval, long recordTs, String point) {
		this.calInterval = calInterval;
		this.recordTs = recordTs;
		this.point = point;
	}

	@Override
	public boolean equals(Object o) {
		UvKey temp = (UvKey) o;
		return this.calInterval == temp.calInterval & recordTs == temp.recordTs
				&& point.equals(temp.point);
	}

	@Override
	public int hashCode() {
		return (int) (calInterval ^ (calInterval >>> 32))
				+ (int) (recordTs ^ (recordTs >>> 32)) + point.hashCode();
	}

	@Override
	public String toString() {
		return "PvKey:" + calInterval + "," + recordTs + "," + point;
	}
}
