CREATE DATABASE `chuangxin` /*!40100 DEFAULT CHARACTER SET utf8mb4 */
ALTER DATABASE chuangxin CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

CREATE TABLE chuangxin.token (
  access_token VARCHAR(255),
  token_type VARCHAR(255),
  refresh_token VARCHAR(255),
  expires_in INT,
  scope VARCHAR(255),
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO chuangxin.token
(access_token, token_type, refresh_token, expires_in, `scope`, create_time, update_time)
VALUES('9dec770d-a19e-4fa5-9b25-eff1b73e30b1', 'bearer', 'e9bf2b38-da24-4212-8596-98106ba6e518', 604799, 'read_cn', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);


CREATE TABLE chuangxin.task (
  task_name VARCHAR(255) comment '任务名称',
  max_dt VARCHAR(255) comment '最大公布日',
  inc_cn VARCHAR(255) comment '增量字段中文',
  inc_col VARCHAR(255) comment '增量字段',
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO chuangxin.task (task_name,max_dt,inc_cn,inc_col) VALUES
	 ('FLINK-SYNC:PATENT_SEARCH_EXPRESSION','19900101','申请日','ad'),
	 ('FLINK-SYNC:PATENT_TRANSFER_SEARCH_EXPRESSION','19900101','申请日','ad'),
	 ('FLINK-SYNC:PATENT_TRANSFER_RECORD_SEARCH_EXPRESSION','19900101','法律公告日','ilsad'),
	 ('FLINK-SYNC:PATENT_PLEDGE_SEARCH_EXPRESSION','19900101','申请日','ad'),
	 ('FLINK-SYNC:PATENT_PLEDGE_RECORD_SEARCH_EXPRESSION','19900101','生效日期','ppedd'),
	 ('FLINK-SYNC:PATENT_PERMIT_SEARCH_EXPRESSION','19900101','申请日','ad'),
	 ('FLINK-SYNC:PATENT_PERMIT_RECORD_SEARCH_EXPRESSION','19900101','合同备案日期','crdd'),
	 ('FLINK-SYNC:PATENT_LAW_STATUS_SEARCH_EXPRESSION','19900101','申请日','ad'),
	 ('FLINK-SYNC:PATENT_LAW_RECORD_STATUS_SEARCH_EXPRESSION','19900101','法律公告日','ilsad');


CREATE TABLE chuangxin.sub_task (
  pid VARCHAR(255) comment '专利编码',
  pno VARCHAR(255) comment '公布号原始',
  patent_detail_status INT DEFAULT 0 comment '专利详情同步状态：0否1是',
  legal_detail_status INT DEFAULT 0 comment '法律状态明细同步状态：0否1是',
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE chuangxin.image_await_task (
  task_name VARCHAR(255) comment '任务名称',
  key_field VARCHAR(255) comment '主键字段',
  key_value VARCHAR(255) comment '主键字段值',
  image_field_name VARCHAR(255) comment '图片字段',
  image_url VARCHAR(255) comment '图片地址',
  down_status INT DEFAULT 0 comment '图片是否下载：0否1是',
  error_info TEXT comment '错误信息',
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

--创建topic
--kafka-topics.sh --zookeeper bigdata01:2181 --delete --topic kafka_image_source_topic
--kafka-topics.sh --create --zookeeper bigdata01:2181 --replication-factor 1 --partitions 2 --topic kafka_image_source_topic