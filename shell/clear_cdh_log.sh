#!/bin/bash

# 清理日志
cd /var/log/cloudera-scm-firehose
rm -rf *.out.* 
rm -rf *.log.*

cd /var/log/cloudera-scm-server
rm -rf *.out.* 
rm -rf *.log.*

cd /var/log/hadoop-yarn
rm -rf *.out.* 
rm -rf *.log.*

cd /var/log/hbase
rm -rf *.out.* 
rm -rf *.log.*

cd /var/log/cloudera-scm-alertpublisher
rm -rf *.out.* 
rm -rf *.log.*

cd /var/log/cloudera-scm-eventserver
rm -rf *.out.* 
rm -rf *.log.*

cd /var/log/kafka
rm -rf *.out.*
rm -rf *.log.*

cd /var/log/zookeeper
rm -rf *.out.*
rm -rf *.log.*

cd /var/log/hadoop-hdfs
rm -rf *.out.*
rm -rf *.log.*

cd /var/log/cloudera-scm-agent
rm -rf *.out.*
rm -rf *.log.*

# 清理监控日志
cd /var/lib/cloudera-host-monitor/ts/type/partitions
rm -rf type*
cd /var/lib/cloudera-host-monitor/ts/stream/partitions
rm -rf stream*
cd /var/lib/cloudera-host-monitor/ts/ts_stream_rollup_PT600S/partitions/
rm -rf ts_stream*
cd /var/lib/cloudera-host-monitor/ts/ts_type_rollup_PT600S/partitions/
rm -rf ts_type*

cd /var/lib/cloudera-service-monitor/ts/stream/partitions/
rm -rf stream*
cd /var/lib/cloudera-service-monitor/ts/type/partitions/
rm -rf type*
cd /var/lib/cloudera-service-monitor/ts/ts_stream_rollup_PT600S/partitions/
rm -rf ts_stream*
cd /var/lib/cloudera-service-monitor/ts/ts_type_rollup_PT600S/partitions/
rm -rf ts_type*

hadoop fs -rm -r .Trash

rm -rf /yarn/nm/usercache/root/filecache/*