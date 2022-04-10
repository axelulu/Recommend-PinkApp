create database if not exists pink comment "PinkApp" location '/user/hive/warehouse/pink.db/';

use pink;

create table posts (
  id BIGINT comment "id",
  post_id BIGINT comment "post_id",
  author_id BIGINT comment "author_id",
  post_type STRING comment "post_type",
  category_id BIGINT comment "category_id",
  title STRING comment "title",
  content STRING comment "content",
  reply BIGINT comment "reply",
  favorite BIGINT comment "favorite",
  likes BIGINT comment "likes",
  un_likes BIGINT comment "un_likes",
  coin BIGINT comment "coin",
  share BIGINT comment "share",
  view BIGINT comment "view",
  cover STRING comment "cover",
  video STRING comment "video",
  download STRING comment "download",
  create_time STRING comment "create_time",
  update_time STRING comment "update_time"
)
COMMENT "PinkApp Posts"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/posts';

create table users (
  `id` BIGINT comment "id",
  `user_id` BIGINT comment "user_id",
  `username` STRING comment "username",
  `password` STRING comment "password",
  `email` STRING comment "email",
  `fans` BIGINT comment "fans",
  `follows` BIGINT comment "follows",
  `coin` BIGINT comment "coin",
  `phone` BIGINT comment "phone",
  `avatar` STRING comment "avatar",
  `background` STRING comment "background",
  `descr` STRING comment "descr",
  `exp` BIGINT comment "exp",
  `gender` BIGINT comment "gender",
  `is_vip` STRING comment "is_vip",
  `birth` STRING comment "birth",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Users"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/users';

create table stars (
  `id` BIGINT comment "id",
  `user_id` BIGINT comment "user_id",
  `post_id` BIGINT comment "post_id",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Stars"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/stars';

create table likes (
  `id` BIGINT comment "id",
  `user_id` BIGINT comment "user_id",
  `post_id` BIGINT comment "post_id",
  `type` STRING comment "type",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Likes"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/likes';

create table comments (
  `id` BIGINT comment "id",
  `user_id` BIGINT comment "user_id",
  `post_id` BIGINT comment "post_id",
  `content` STRING comment "content",
  `type` STRING comment "type",
  `parent` BIGINT comment "parent",
  `like_num` BIGINT comment "like_num",
  `status` BIGINT comment "status",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Comments"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/comments';

create table coins (
  `id` BIGINT comment "id",
  `user_id` BIGINT comment "user_id",
  `post_id` BIGINT comment "post_id",
  `num` BIGINT comment "num",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Coins"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/coins';

create table follows (
  `id` BIGINT comment "id",
  `user_id` BIGINT comment "user_id",
  `follow_id` BIGINT comment "follow_id",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Follows"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/follows';

create table categories (
  `id` BIGINT comment "id",
  `category_id` BIGINT comment "category_id",
  `category_name` STRING comment "category_name",
  `create_time` STRING comment "create_time",
  `update_time` STRING comment "update_time"
)
COMMENT "PinkApp Categories"
row format delimited fields terminated by ','
LOCATION '/user/hive/warehouse/pink.db/categories';
