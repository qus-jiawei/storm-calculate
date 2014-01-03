package cn.uc.storm.cal.values;


public class PvKey {
	public final long recordTs;
	public final String point;
	public PvKey(long recordTs,String point){
		this.recordTs = recordTs;
		this.point = point;
	}
	@Override
	public boolean equals(Object o) {
		PvKey temp = (PvKey) o;
		return recordTs== temp.recordTs && point.equals(temp.point);
	}
	@Override
	public int hashCode(){
		return (int)(recordTs^(recordTs>>>32)) + point.hashCode();
	}
	@Override
	public String toString(){
		return "PvKey:"+recordTs+","+point;
	}
}
