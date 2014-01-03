package cn.uc.storm.cal.values;

public class PvPoint {
	public final PvKey pvKey;
	public final PvValue pvValue;
	public PvPoint(PvKey pvKey,PvValue pvValue){
		this.pvKey = pvKey;
		this.pvValue = pvValue;
	}
	@Override
	public String toString(){
		return pvKey+","+pvValue;
	}
}
