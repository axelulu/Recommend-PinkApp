create 'article_similar', 'similar'

create 'cb_recall', {NAME=>'als', TTL=>7776000, VERSIONS=>999999}
alter 'cb_recall', {NAME=>'content', TTL=>7776000, VERSIONS=>999999}
alter 'cb_recall', {NAME=>'online', TTL=>7776000, VERSIONS=>999999}

create 'history_recall', {NAME=>'category', TTL=>7776000, VERSIONS=>999999}

create 'user_profile', 'basic','partial','env'

create 'ctr_feature_user', 'category'

create 'ctr_feature_article', 'article'

create 'wait_recommend', 'category'

create 'history_recommend', {NAME=>'category', TTL=>7776000, VERSIONS=>999999}