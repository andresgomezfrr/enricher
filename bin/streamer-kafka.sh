#!/usr/bin/env bash

if [ $# -lt 1 ];
then
    echo "USAGE: $0 <bootstrap_kafka_server> <app_id> [stream_config_path]"
    exit 1
fi

CURRENT=`pwd` && cd `dirname $0` && SOURCE=`pwd` && cd ${CURRENT} && PARENT=`dirname ${SOURCE}`

for file in ${PARENT}/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

if [ $# -eq 2 ]; then
  java -cp ${CLASSPATH} zz.ks.utils.bootstrap.StreamerKafkaConfig $1 $2
elif [ $# -eq 3 ]; then
  java -cp ${CLASSPATH} zz.ks.utils.bootstrap.StreamerKafkaConfig $1 $2 $3
fi