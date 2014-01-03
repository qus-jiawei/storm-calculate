package cn.uc.storm.cal.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class PointCache<K1, K2> implements Iterator {
	private class Key {
		private K1 k1;
		private K2 k2;

		public Key(K1 k1, K2 k2) {
			this.k1 = k1;
			this.k2 = k2;
		}

		@Override
		public boolean equals(Object o) {
			Key temp = (Key) o;
			return k1.equals(temp.k1) && k2.equals(temp.k2);
		}
		@Override
		public int hashCode(){
			return k1.hashCode()+k2.hashCode();
		}
	}

	private class IncInteger {
		private int count = 0;

		public IncInteger() {
		}

		public void inc(int count) {
			this.count += count;
		}
	}

	public class ResultPoint {
		public final K1 k1;
		public final K2 k2;
		public final int count;

		public ResultPoint(K1 k1, K2 k2, int count) {
			this.k1 = k1;
			this.k2 = k2;
			this.count = count;
		}
		@Override
		public String toString(){
			return k1.toString()+","+k2.toString()+","+count;
		}
	}

	private Map<Key, IncInteger> map;

	public PointCache() {
		map = new HashMap<Key, IncInteger>();
	}

	public void inc(K1 k1, K2 k2, int count) {
		Key key = new Key(k1, k2);
		IncInteger ii = map.get(key);
		if (ii == null) {
			ii = new IncInteger();
			map.put(key, ii);
		}
		ii.inc(count);
	}

	private Iterator<Entry<Key, IncInteger>> iter;

	@Override
	public boolean hasNext() {
		if (iter == null) {
			iter = map.entrySet().iterator();
		}
		return iter.hasNext();
	}

	@Override
	public Object next() {
		Entry<Key, IncInteger> entry = iter.next();
		return new ResultPoint(entry.getKey().k1, entry.getKey().k2,
				entry.getValue().count);
	}

	@Override
	public void remove() {

	}
}
