time=`date +"%Y-%m-%d" -d "-2year"`
declare -A check
check=([comments]=update_time [stars]=update_time [likes]=update_time [follows]=update_time [coins]=update_time [categories]=update_time)
declare -A merge
merge=([comments]=id [stars]=id [likes]=id [follows]=id [coins]=id [categories]=id)

# posts
sqoop import \
    --connect jdbc:mysql://hadoop102:3306/pink?tinyInt1isBit=false \
    --username root \
    --password zhaolu123 \
    --mapreduce-job-name 'posts' \
    --m 1 \
    --query 'select id, post_id, author_id, post_type, category_id, REPLACE(REPLACE(REPLACE(title, CHAR(13),""),CHAR(10),""), ",", " ") title, REPLACE(REPLACE(REPLACE(content, CHAR(13),""),CHAR(10),""), ",", " ") content, reply, favorite, likes, un_likes, coin, share, view, cover, REPLACE(REPLACE(REPLACE(video, CHAR(13),""),CHAR(10),""), ",", " ") video, REPLACE(REPLACE(REPLACE(download, CHAR(13),""),CHAR(10),""), ",", " ") download, create_time, update_time from posts WHERE $CONDITIONS' \
    --split-by post_id \
    --target-dir /user/hive/warehouse/pink.db/posts \
    --incremental lastmodified \
    --check-column update_time \
    --merge-key post_id \
    --last-value ${time}

# users
sqoop import \
    --connect jdbc:mysql://hadoop102:3306/pink?tinyInt1isBit=false \
    --username root \
    --password zhaolu123 \
    --mapreduce-job-name 'users' \
    --m 1 \
    --query 'select id, user_id, REPLACE(REPLACE(REPLACE(username, CHAR(13),""),CHAR(10),""), ",", " ") username, password, email, fans, follows, coin, phone, avatar, background, REPLACE(REPLACE(REPLACE(descr, CHAR(13),""),CHAR(10),""), ",", " ") descr, exp, gender, is_vip, birth, create_time, update_time from users WHERE $CONDITIONS' \
    --split-by user_id \
    --target-dir /user/hive/warehouse/pink.db/users \
    --incremental lastmodified \
    --check-column update_time \
    --merge-key user_id \
    --last-value ${time}

for k in ${!check[@]}
do
    sqoop import \
        --connect jdbc:mysql://hadoop102:3306/pink \
        --username root \
        --password zhaolu123 \
        --table $k \
        --m 1 \
        --target-dir /user/hive/warehouse/pink.db/$k \
        --incremental lastmodified \
        --check-column ${check[$k]} \
        --merge-key ${merge[$k]} \
        --last-value ${time}
done
