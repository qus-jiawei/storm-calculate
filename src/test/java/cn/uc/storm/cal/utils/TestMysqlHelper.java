package cn.uc.storm.cal.utils;

import org.junit.Test;

import cn.uc.storm.cal.values.PvKey;
import cn.uc.storm.cal.values.PvPoint;
import cn.uc.storm.cal.values.PvValue;
import cn.uc.storm.cal.values.UvKey;
import cn.uc.storm.cal.values.UvPoint;
import cn.uc.storm.cal.values.UvValue;

public class TestMysqlHelper {

	@Test
	public void test() {
		System.out.println(MysqlHelper.getCreatePvTable("pv"));
		PvPoint pvPoint = new PvPoint(new PvKey(System.currentTimeMillis(),"abcd"),new PvValue(100));
		System.out.println(MysqlHelper.getUpdateSqlFromPvPoint("pv", pvPoint));
		System.out.println(MysqlHelper.getCreateUvTable("uv"));
		UvPoint uvPoint = new UvPoint(new UvKey(600000,System.currentTimeMillis(),"abcd"),new UvValue(100));
		System.out.println(MysqlHelper.getUpdateSqlFromUvPoint("uv", uvPoint));
	}
	
}
