/*
Navicat MySQL Data Transfer

Source Server         : 192.168.1.34
Source Server Version : 50528
Source Host           : 192.168.1.34:3306
Source Database       : db_platform

Target Server Type    : MYSQL
Target Server Version : 50528
File Encoding         : 65001

Date: 2014-09-04 20:34:52
*/
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for `t_database`
-- ----------------------------
DROP TABLE
IF EXISTS `t_database`;

CREATE TABLE `t_database` (
	`id` INT (11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
	`ip` CHAR (15) NOT NULL DEFAULT '' COMMENT '数据库IP',
	`port` INT (11) NOT NULL DEFAULT 0 COMMENT '数据库端口',
	`dbtype` VARCHAR (20) NOT NULL DEFAULT '' COMMENT '数据库类型',
	`dbname` VARCHAR (100) NOT NULL DEFAULT '' COMMENT '数据库名',
	`is_backup` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '是否需要备份，0->不需要,1->需要,99->初始化状态',
	`is_recover` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '是否需要恢复测试，0->不需要,1->需要,99->初始化状态',
	`is_monitor` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '是否需要部署aagent监控，0->不需要,1->需要,99->初始化状态',
	`is_online` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '是否在线，0->下线,1->在线,99->初始化状态',
	`note` VARCHAR (200) NOT NULL DEFAULT '' COMMENT '备注描述',
	`expired_role` VARCHAR (20) NOT NULL DEFAULT '7' COMMENT '异地备份过期规则,使用;间隔三段，分别表示天，周，月对应过期天数，#表示不过期',
	`has_rqmbackup` TINYINT (4) NOT NULL DEFAULT 0 COMMENT '部署rqmbackup状态：0 未部署 1已部署',
	PRIMARY KEY (`id`),
	UNIQUE INDEX `uniq_idx_1` (`ip`, `port`, `dbname`) USING BTREE,
	INDEX `idx_2` (`is_online`) USING BTREE,
	INDEX `idx_dt` (`dbtype`) USING BTREE
) ENGINE = INNODB DEFAULT CHARACTER
SET = utf8 COLLATE = utf8_general_ci AUTO_INCREMENT = 1;

-- ----------------------------
-- Table structure for `t_database_backup_info`
-- ----------------------------
DROP TABLE
IF EXISTS `t_database_backup_info`;

CREATE TABLE `t_database_backup_info` (
	`id` INT (11) NOT NULL AUTO_INCREMENT,
	`did` INT (11) NULL DEFAULT NULL COMMENT '对应数据库ID t_database',
	`bind_ip` VARCHAR (20) NULL DEFAULT NULL COMMENT '实例监听ip',
	`inst_type` enum (
		'redis-server',
		'tnslsnr',
		'memcached',
		'mongod',
		'oracle',
		'mysqld'
	) NULL DEFAULT NULL COMMENT '实例类型',
	`inst_port` INT (11) NULL DEFAULT NULL COMMENT '实例端口',
	`inst_version` VARCHAR (20) NULL DEFAULT NULL COMMENT '实例版本',
	`dbname` VARCHAR (50) NULL DEFAULT NULL COMMENT '数据库名称',
	`backup_tool` VARCHAR (20) NULL DEFAULT NULL COMMENT '备份工具名称',
	`file_name` VARCHAR (100) NULL DEFAULT NULL COMMENT '备份结果文件名',
	`file_count` TINYINT (5) UNSIGNED NULL DEFAULT 0 COMMENT '备份文件数量',
	`file_size` INT (11) NULL DEFAULT NULL COMMENT '备份结果文件大小',
	`file_md5` VARCHAR (50) NULL DEFAULT NULL COMMENT '备份结果md5值',
	`bak_start` TIMESTAMP NULL DEFAULT NULL COMMENT '备份开始时间',
	`bak_stop` TIMESTAMP NULL DEFAULT NULL COMMENT '备份结束时间',
	`bak_status` TINYINT (4) NULL DEFAULT 0 COMMENT '备份状态： 0 未开始 1 开始未结束 2 结束正确 -1 错误',
	`rsync_start` TIMESTAMP NULL DEFAULT NULL COMMENT '异地传输开始时间',
	`rsync_stop` TIMESTAMP NULL DEFAULT NULL COMMENT '异地传输结束时间',
	`rsync_path` VARCHAR (150) NULL DEFAULT NULL COMMENT '异地传输备份路径 ssh://10.1.192.4/data/backup/',
	`rsync_status` TINYINT (4) NULL DEFAULT 0 COMMENT '异地传输状态：0 未开始 1开始未结束 2 结束正确 -1 有问题',
	`rsync_expired` date NOT NULL DEFAULT '9999-12-31',
	`recover_start` TIMESTAMP NULL DEFAULT NULL COMMENT '最近恢复测试开始时间',
	`recover_end` TIMESTAMP NULL DEFAULT NULL COMMENT '最近恢复测试结束时间',
	`recover_status` TINYINT (4) NULL DEFAULT 0 COMMENT '恢复测试状态 0 未开始 1 已开始未结束 2 结束正确 -1 结束错误',
	`is_deleted` TINYINT (4) NOT NULL DEFAULT 0,
	PRIMARY KEY (`id`)
) ENGINE = INNODB DEFAULT CHARACTER
SET = utf8 COLLATE = utf8_general_ci AUTO_INCREMENT = 1;