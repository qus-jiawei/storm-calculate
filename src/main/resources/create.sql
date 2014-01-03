-- 统计结果表格斯约定


CREATE TABLE IF NOT EXISTS pv (
 `recordTs` DATETIME NOT NULL COMMENT '记录时间，格式是unix时间戳',
 `point` VARCHAR(256) NOT NULL COMMENT '指标的唯一区分id' ,
 `value` INT COMMENT '统计值' ,
 PRIMARY KEY (`recordTs`,`point`) 
)DEFAULT CHARACTER SET=utf8 ;
-- 统计周期默认是5分钟
INSERT INTO pv (`recordTs`,`point`,`value`) VALUES ( "20131231150000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO pv (`recordTs`,`point`,`value`) VALUES ( "20131231150000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO pv (`recordTs`,`point`,`value`) VALUES ( "20131231150500" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO pv (`recordTs`,`point`,`value`) VALUES ( "20131231150500" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;


-- 统计周期分别是5分钟和1小时 1天
CREATE TABLE IF NOT EXISTS uv (
 `calInterval` INT NOT NULL COMMENT '统计周期,单位是秒' ,
 `recordTs` DATETIME NOT NULL COMMENT '记录时间，格式是unix时间戳' ,
 `point` VARCHAR(256) NOT NULL COMMENT '指标的唯一区分id',
 `value` INT COMMENT '统计值' ,
 PRIMARY KEY (`calInterval`,`recordTs`,`point`) 
 ) DEFAULT CHARACTER SET=utf8 ;

-- 5分钟的更新语句
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 300 , "20131231150000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 300 , "20131231150000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 300 , "20131231150500" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 300 , "20131231150500" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
-- 1小时的更新语句
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 3600 , "20131231150000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 3600 , "20131231150000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 3600 , "20131231160000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 3600 , "20131231160000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
-- 1天的更新语句
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 86400 , "20131231000000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 86400 , "20131231000000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 86400 , "20140101000000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;
INSERT INTO uv (`calInterval`,`recordTs`,`point`,`value`) VALUES ( 86400 , "20140101000000" , "指标A" , 100 ) ON DUPLICATE KEY UPDATE  `value` = `value` + 100;