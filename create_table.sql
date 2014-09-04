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
	`id` INT (11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '��������',
	`ip` CHAR (15) NOT NULL DEFAULT '' COMMENT '���ݿ�IP',
	`port` INT (11) NOT NULL DEFAULT 0 COMMENT '���ݿ�˿�',
	`dbtype` VARCHAR (20) NOT NULL DEFAULT '' COMMENT '���ݿ�����',
	`dbname` VARCHAR (100) NOT NULL DEFAULT '' COMMENT '���ݿ���',
	`is_backup` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '�Ƿ���Ҫ���ݣ�0->����Ҫ,1->��Ҫ,99->��ʼ��״̬',
	`is_recover` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '�Ƿ���Ҫ�ָ����ԣ�0->����Ҫ,1->��Ҫ,99->��ʼ��״̬',
	`is_monitor` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '�Ƿ���Ҫ����aagent��أ�0->����Ҫ,1->��Ҫ,99->��ʼ��״̬',
	`is_online` TINYINT (2) NOT NULL DEFAULT 99 COMMENT '�Ƿ����ߣ�0->����,1->����,99->��ʼ��״̬',
	`note` VARCHAR (200) NOT NULL DEFAULT '' COMMENT '��ע����',
	`expired_role` VARCHAR (20) NOT NULL DEFAULT '7' COMMENT '��ر��ݹ��ڹ���,ʹ��;������Σ��ֱ��ʾ�죬�ܣ��¶�Ӧ����������#��ʾ������',
	`has_rqmbackup` TINYINT (4) NOT NULL DEFAULT 0 COMMENT '����rqmbackup״̬��0 δ���� 1�Ѳ���',
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
	`did` INT (11) NULL DEFAULT NULL COMMENT '��Ӧ���ݿ�ID t_database',
	`bind_ip` VARCHAR (20) NULL DEFAULT NULL COMMENT 'ʵ������ip',
	`inst_type` enum (
		'redis-server',
		'tnslsnr',
		'memcached',
		'mongod',
		'oracle',
		'mysqld'
	) NULL DEFAULT NULL COMMENT 'ʵ������',
	`inst_port` INT (11) NULL DEFAULT NULL COMMENT 'ʵ���˿�',
	`inst_version` VARCHAR (20) NULL DEFAULT NULL COMMENT 'ʵ���汾',
	`dbname` VARCHAR (50) NULL DEFAULT NULL COMMENT '���ݿ�����',
	`backup_tool` VARCHAR (20) NULL DEFAULT NULL COMMENT '���ݹ�������',
	`file_name` VARCHAR (100) NULL DEFAULT NULL COMMENT '���ݽ���ļ���',
	`file_count` TINYINT (5) UNSIGNED NULL DEFAULT 0 COMMENT '�����ļ�����',
	`file_size` INT (11) NULL DEFAULT NULL COMMENT '���ݽ���ļ���С',
	`file_md5` VARCHAR (50) NULL DEFAULT NULL COMMENT '���ݽ��md5ֵ',
	`bak_start` TIMESTAMP NULL DEFAULT NULL COMMENT '���ݿ�ʼʱ��',
	`bak_stop` TIMESTAMP NULL DEFAULT NULL COMMENT '���ݽ���ʱ��',
	`bak_status` TINYINT (4) NULL DEFAULT 0 COMMENT '����״̬�� 0 δ��ʼ 1 ��ʼδ���� 2 ������ȷ -1 ����',
	`rsync_start` TIMESTAMP NULL DEFAULT NULL COMMENT '��ش��俪ʼʱ��',
	`rsync_stop` TIMESTAMP NULL DEFAULT NULL COMMENT '��ش������ʱ��',
	`rsync_path` VARCHAR (150) NULL DEFAULT NULL COMMENT '��ش��䱸��·�� ssh://10.1.192.4/data/backup/',
	`rsync_status` TINYINT (4) NULL DEFAULT 0 COMMENT '��ش���״̬��0 δ��ʼ 1��ʼδ���� 2 ������ȷ -1 ������',
	`rsync_expired` date NOT NULL DEFAULT '9999-12-31',
	`recover_start` TIMESTAMP NULL DEFAULT NULL COMMENT '����ָ����Կ�ʼʱ��',
	`recover_end` TIMESTAMP NULL DEFAULT NULL COMMENT '����ָ����Խ���ʱ��',
	`recover_status` TINYINT (4) NULL DEFAULT 0 COMMENT '�ָ�����״̬ 0 δ��ʼ 1 �ѿ�ʼδ���� 2 ������ȷ -1 ��������',
	`is_deleted` TINYINT (4) NOT NULL DEFAULT 0,
	PRIMARY KEY (`id`)
) ENGINE = INNODB DEFAULT CHARACTER
SET = utf8 COLLATE = utf8_general_ci AUTO_INCREMENT = 1;