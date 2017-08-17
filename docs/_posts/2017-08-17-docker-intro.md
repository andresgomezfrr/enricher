---
layout: page
title: "Docker Intro"
category: docker
date: 2017-08-17 11:17:33
---

Enricher image: `gcr.io/wizzie-registry/enricher:0.1.0`

You can configure the docker image using these environment properties:

| Env Property   |      Description      |  Default Value |
|----------|---------------|-------|
| `APPLICATION_ID` |  Enable or disable metrics service | |
| `KAFKA_BOOTSTRAP_SERVER` |  Kafka servers |  |
| `NUM_STREAM_THREADS` |  Number parallelism | 1|
| `METRIC_ENABLE` | Enable the metrics |  true  |
| `METRIC_INTERVAL`|The interval time to report metrics (milliseconds) | 60000 |
| `MULTI_ID`| Configure the multi.id property [Multi Tenant](http://www.wizzie.io/enricher/conf/multi-tenant.html) | false |
| `MAX_OUTPUT_KAFKA_TOPICS`| Enable the verbose metric mode | 500 |

An example to execute the docker image is:

```
docker run -it -e APPLICATION_ID=my-app -e KAFKA_BOOTSTRAP_SERVER=kafka.server.io:9092 gcr.io/wizzie-registry/enricher:latest
```

