package cn.uc.storm.cal.utils;

import java.text.SimpleDateFormat;

import cn.uc.storm.cal.values.PvPoint;
import cn.uc.storm.cal.values.UvPoint;

public class MysqlHelper {

	static public String getCreatePvTable(String tableName) {
		return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
				+ "`recordTs` DATETIME NOT NULL COMMENT '记录时间，格式是unix时间戳，一定是pv统计周期的整数倍', "
				+ "`point` VARCHAR(256) NOT NULL COMMENT '指标的唯一区分id' , " 
				+ "`value` INT COMMENT '统计值' , "
				+ "PRIMARY KEY (`recordTs`,`point`) "
				+ ")"
				+ "DEFAULT CHARACTER SET=utf8 "
				;
	}

	// INSERT INTO t (t.a, t.b, t.c)
	// VALUES ('key1','key2','value')
	// ON DUPLICATE KEY UPDATE
	// t.c = 'value';
	static public String getUpdateSqlFromPvPoint(String tableName,
			PvPoint pvPoint) {
		return "INSERT INTO " + tableName + " (`recordTs`,`point`,`value`) "
				+ "VALUES ( " + DateHelper.getDateTimeFromTs(pvPoint.pvKey.recordTs) + " , \""
				+ pvPoint.pvKey.point + "\" , " + pvPoint.pvValue.getConut()
				+ " ) ON DUPLICATE KEY UPDATE  `value` = `value` + "
				+ pvPoint.pvValue.getConut();
	}

	static public String getCreateUvTable(String tableName) {
		return "CREATE TABLE IF NOT EXISTS " + tableName + " ("
				+ "`calInterval` INT NOT NULL COMMENT '统计周期,单位是秒' , "
				+ "`recordTs` DATETIME NOT NULL COMMENT '记录时间，格式是unix时间戳，一定是calInterval的整数倍' , "
				+ "`point` VARCHAR(256) NOT NULL COMMENT '指标的唯一区分id', " 
				+ "`value` INT COMMENT '统计值' , "
				+ "PRIMARY KEY (`calInterval`,`recordTs`,`point`) "
				+ ") "
				+ "DEFAULT CHARACTER SET=utf8 ";
	}
	
	static public String getUpdateSqlFromUvPoint(String tableName,
			UvPoint uvPoint) {
		return "INSERT INTO " + tableName + " (`calInterval`,`recordTs`,`point`,`value`) "
				+ "VALUES ( " 
				+ (uvPoint.uvKey.calInterval/1000) + " , "
				+ DateHelper.getDateTimeFromTs(uvPoint.uvKey.recordTs) + " , "
				+ "\"" + uvPoint.uvKey.point + "\" , "
				+ uvPoint.uvValue.getConut()
				+ " ) ON DUPLICATE KEY UPDATE  `value` = `value` + "
				+ uvPoint.uvValue.getConut();
	}
	
}
