---
layout: page
title: "Kafka Bootstrapper"
category: bootstrapper
date: 2017-02-21 10:24:06
order: 3
---

`io.wizzie.ks.enricher.builder.bootstrap.KafkaBootstrapper`

This bootstrapper reads the stream config from Kafka, so you can change the stream topology without restart the service. The bootstrapper is reading the topic `__enricher_bootstrap` using a kafka consumer instance with a random `group.id`.

### StreamerKafkaConfig

The StreamerKafkaConfig tool allows us to send new stream config to the enricher and to read the current stream config. 

You can use the script [streamer-kafka.sh](https://github.com/wizzie-io/enricher/blob/master/bin/streamer-kafka.sh) to use this tool. The tool has two modes:

#### Read Mode

The read mode allows us to read the current stream configuration to specific enricher instance. 

```bash
bin/streamer-kafka.sh $BOOTSTRAP_KAFKA_SERVER $APPLICATION_ID
```

#### Write Mode

The write mode allows us to send new stream configuration to specific enricher instance.

```bash
bin/streamer-kafka.sh $BOOTSTRAP_KAFKA_SERVER $APPLICATION_ID $STREAM_CONFIG_FILE

