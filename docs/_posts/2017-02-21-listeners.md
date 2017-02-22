---
layout: page
title: "Listeners"
category: metrics
date: 2017-02-21 10:18:32
order: 2
---

The listeners are the processes that listen the reported metrics and do something with them. You can have multiple listeners at the same time. Currently, the enricher service supports two listener metrics:

#### ConsoleMetricListener

This listener `io.wizzie.ks.metrics.ConsoleMetricListener` sends the transform the metrics to JSON and prints them into the log file using the log4j. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345}
```

#### KafkaMetricListener
This listener `io.wizzie.ks.enricher.metrics.KafkaMetricListener` sends the transform the metrics to JSON and sends them into the Kafka topic. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345, "app_id":"MY_KAFKA_STREAMS_APP_ID"}
```

This listener adds a new property to specify the metrics Kafka topic `metric.kafka.topic`, by default is `__enricher_metrics`

#### Custom Listeners
You can make new listeners. To do this you need to implement the [MetricListener Class](https://github.com/wizzie-io/enricher/blob/master/service/src/main/java/io/wizzie/ks/enricher/metrics/MetricListener.java). On this class you receive the metric at the method `void updateMetric(String metricName, Object metricValue);`

