package cn.uc.storm.cal.main;

public class Constants {
	//Topology的spout和Bolt的id
	static public final String SPOUT_STREAM_ID = "SPOUT";
	static public final String PV_STREAM_ID = "PV";
	static public final String UV_STREAM_ID = "UV";
	static public final String SPOUT_ID = "calSpoutId";
	static public final String CAL_BOLT_ID = "CalBoltId";
	static public final String PV_BOLT_ID = "pvBoltId";
	static public final String PV_SUM_BOLT_ID = "pvSumBoltId";
	static public final String UV_BOLT_ID = "uvBoltId";
	static public final String UV_SUM_BOLT_ID = "uvSumBoltId";
	
	//各配置项
	static public final String PV_UPDATE_INTERVAL = "uc.storm.pv.update.interval";
	static public final String PV_CAL_INTERVAL = "uc.storm.pv.cal.interval";
	static public final String UV_UPDATE_INTERVAL = "uc.storm.uv.update.interval";
	static public final String UV_CAL_INTERVAL = "uc.storm.uv.cal.interval";
	
	static public final String LOG_TIME_OUT = "uc.storm.log.timeout";
	
	//各种类定义
	static public final String TOPOLOGY_ID = "uc.storm.cal.topology.id";
		
	static public final String CAL_SPOUT =	"uc.storm.cal.spout";
	static public final String CAL_SPOUT_NUM =	"uc.storm.cal.spout.num";
	
	static public final String CAL_CALBOLT =	"uc.storm.cal.calbolt";
	static public final String CAL_CALBOLT_NUM =	"uc.storm.cal.calbolt.num";
	
	static public final String CAL_PVBOLT =	"uc.storm.cal.pvbolt";
	static public final String CAL_PVBOLT_NUM =	"uc.storm.cal.pvbolt.num";
	
	static public final String CAL_PVSUMBOLT =	"uc.storm.cal.pvsumbolt";
	static public final String CAL_PVSUMBOLT_NUM =	"uc.storm.cal.pvsumbolt.num";

	static public final String CAL_UVBOLT =	"uc.storm.cal.uvbolt";
	static public final String CAL_UVBOLT_NUM =	"uc.storm.cal.uvbolt.num";
	
	static public final String CAL_UVSUMBOLT =	"uc.storm.cal.uvsumbolt";
	static public final String CAL_UVSUMBOLT_NUM =	"uc.storm.cal.uvsumbolt.num";
	
	static public final String LOCAL_MODE = "uc.storm.local";
	
	static public final long FIRST_FLUSH_DELAY = 5000L;
}
