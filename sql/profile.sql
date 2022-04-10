create database if not exists profile comment "use action" location '/user/hive/warehouse/profile.db/';
use profile;
create table user_action(
level STRING comment "user level",
userTime STRING comment "user time",
caller STRING comment "user caller",
msg STRING comment "user msg",
actionTime STRING comment "user actions time",
readTime STRING comment "user reading time",
category_id BIGINT comment "article category_id",
param struct<action:STRING, userId:STRING, postId:STRING, algorithmCombine:STRING> comment "action parameter")
COMMENT "user primitive action"
PARTITIONED BY(dt STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hive/warehouse/profile.db/user_action';

create table user_article_basic(
user_id BIGINT comment "userID",
action_time STRING comment "user actions time",
post_id BIGINT comment "post_id",
category_id INT comment "category_id",
shared BOOLEAN comment "is shared",
clicked BOOLEAN comment "is clicked",
collected BOOLEAN comment "is collected",
exposure BOOLEAN comment "is exposured",
read_time STRING comment "reading time")
COMMENT "user_article_basic"
CLUSTERED by (user_id) into 2 buckets
STORED as textfile
LOCATION '/user/hive/warehouse/profile.db/user_article_basic';

create external table user_profile_hbase(
user_id STRING comment "userID",
information map<string, DOUBLE> comment "user basic information",
article_partial map<string, DOUBLE> comment "article partial",
env map<string, INT> comment "user env")
COMMENT "user profile table"
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,basic:,partial:,env:")
TBLPROPERTIES ("hbase.table.name" = "user_profile");


