CREATE TABLE chuangxin.token (
  access_token VARCHAR(255),
  token_type VARCHAR(255),
  refresh_token VARCHAR(255),
  expires_in INT,
  scope VARCHAR(255),
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE chuangxin.task (
  task_name VARCHAR(255) comment '任务名称',
  max_dt VARCHAR(255) comment '最大公布日',
  inc_col VARCHAR(255) comment '增量字段',
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

insert into task (task_name,max_dt,inc_col) values('FLINK-SYNC:PATENT_SEARCH_EXPRESSION','20230601','公布日')
,('FLINK-SYNC:PATENT_LAW_STATUS_SEARCH_EXPRESSION','20230601','公布日')
,('FLINK-SYNC:PATENT_LAW_RECORD_STATUS_SEARCH_EXPRESSION','20230601','法律公告日');

CREATE TABLE chuangxin.sub_task (
  pid VARCHAR(255) comment '专利编码',
  pno VARCHAR(255) comment '公布号原始',
  patent_detail_status INT DEFAULT 0 comment '专利详情同步状态：0否1是',
  transfer_status INT DEFAULT 0 comment '转让信息同步状态：0否1是',
  pledge_status INT DEFAULT 0 comment '质押信息同步状态：0否1是',
  license_status INT DEFAULT 0 comment '许可信息同步状态：0否1是',
  legal_status INT DEFAULT 0 comment '法律状态同步状态：0否1是',
  legal_detail_status INT DEFAULT 0 comment '法律状态明细同步状态：0否1是',
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);