
create database if not exists article comment "article information" location '/user/hive/warehouse/article.db/';
use article;
CREATE TABLE article_data(
post_id BIGINT comment "post_id",
category_id INT comment "category_id",
category_name STRING comment "category_name",
title STRING comment "title",
content STRING comment "content",
sentence STRING comment "sentence")
COMMENT "pink article_data"
LOCATION '/user/hive/warehouse/article.db/article_data';

CREATE TABLE tfidf_keywords_values(
post_id BIGINT comment "post_id",
category_id INT comment "category_id",
keyword STRING comment "keyword",
tfidf DOUBLE comment "tfidf");

CREATE TABLE idf_keywords_values(
keyword STRING comment "keyword",
idf DOUBLE comment "idf",
index INT comment "index");

CREATE TABLE textrank_keywords_values(
post_id BIGINT comment "post_id",
category_id INT comment "category_id",
keyword STRING comment "keyword",
textrank DOUBLE comment "textrank");

CREATE TABLE article_profile(
post_id BIGINT comment "post_id",
category_id INT comment "category_id",
keywords map<string, double> comment "keywords",
topics array<string> comment "topics");

CREATE TABLE article_vector(
post_id BIGINT comment "post_id",
category_id INT comment "category_id",
articlevector array<string> comment "keyword");