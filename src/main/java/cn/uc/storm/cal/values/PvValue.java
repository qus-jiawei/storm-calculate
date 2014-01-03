package cn.uc.storm.cal.values;

public class PvValue {
	private int count;
	public PvValue(){
		
	}
	public PvValue(int count){
		this.count = count;
	}
	public void inc(int count) {
		this.count += count;
	}
	public int	getConut(){
		return count;
	}
	@Override
	public String toString(){
		return "PvValue:"+count;
	}
}
