package cn.uc.storm.cal.values;

public class UvValue {
	private int count;

	public UvValue() {

	}

	public UvValue(int count) {
		this.count = count;
	}

	public void inc(int count) {
		this.count += count;
	}

	public int getConut() {
		return count;
	}

	@Override
	public String toString() {
		return "UvValue:" + count;
	}
}
