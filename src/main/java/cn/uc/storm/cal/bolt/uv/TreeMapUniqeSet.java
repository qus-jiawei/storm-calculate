package cn.uc.storm.cal.bolt.uv;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TreeMapUniqeSet implements UniqeSet {
	
	private Set<String> set;
	public TreeMapUniqeSet(){
		set = new TreeSet<String>();
	}
	@Override
	public void initFromConf(Map conf) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean add(String uniqe) {
		return set.add(uniqe);
	}

	@Override
	public void clean() {
		//do nothing
	}
	

}
