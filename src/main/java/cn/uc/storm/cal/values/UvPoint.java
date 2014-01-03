package cn.uc.storm.cal.values;

public class UvPoint {
	public final UvKey uvKey;
	public final UvValue uvValue;
	public UvPoint(UvKey uvKey,UvValue uvValue){
		this.uvKey = uvKey;
		this.uvValue = uvValue;
	}
	@Override
	public String toString(){
		return uvKey+","+uvValue;
	}
}
