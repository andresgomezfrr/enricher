---
layout: page
title: "Listeners"
category: metrics
date: 2017-02-21 10:18:32
order: 2
---

The Normalizer service use this library to report metrics:

[metrics-library](https://github.com/wizzie-io/metrics-library)

Currently, the enricher service uses two listener metrics:

#### ConsoleMetricListener

This listener `io.wizzie.metrics.listeners.ConsoleMetricListener` sends the transform the metrics to JSON and prints them into the log file using the log4j. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345}
```

#### KafkaMetricListener
This listener `io.wizzie.metrics.listeners.KafkaMetricListener` sends the transform the metrics to JSON and sends them into the Kafka topic. The metric format is:
```json
{"timestamp":123456789, "monitor":"heap-memory", "value":12345, "app_id":"MY_KAFKA_STREAMS_APP_ID"}
```

The `metric.kafka.topic` used by default is `__enricher_metrics`
