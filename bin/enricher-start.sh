#!/usr/bin/env bash

if [ $# -lt 1 ];
then
        echo "USAGE: $0 config.json"
        exit 1
fi

CURRENT=`pwd` && cd `dirname $0` && SOURCE=`pwd` && cd ${CURRENT} && PARENT=`dirname ${SOURCE}`

CLASSPATH=${CLASSPATH}:${PARENT}/config
for file in ${PARENT}/lib/*.jar;
do
    CLASSPATH=${CLASSPATH}:${file}
done

java -cp ${CLASSPATH} io.wizzie.ks.enricher.Enricher $1