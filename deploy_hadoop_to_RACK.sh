#!/bin/bash
echo "Assegurar hadoop apagat"
#ssh -p 2100 -l hadoop localhost /home/hadoop/hadoop-dir/hadoop-2.5.1/sbin/stop-all.sh

echo "Propagar projecte Hadoop a rack"
ssh hadoop@stacksync.urv.cat rm -r /home/hadoop/hadoop-dir/hadoop-2.5.1
scp -r "$HADOOP_HOME" hadoop@stacksync.urv.cat:/home/hadoop/hadoop-dir/

echo "Propagar fitxers configuracio"
./deploy_config_files_to_RACK.sh


echo "Formatar namenode"
#ssh -p 2100 -l hadoop localhost /home/hadoop/hadoop-dir/hadoop-2.5.1/bin/hdfs namenode -format

echo " "
echo "Tot replicat i preparat"
