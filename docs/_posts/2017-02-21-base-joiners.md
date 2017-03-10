---
layout: page
title: "Base Joiners"
category: joiners
date: 2017-02-21 10:43:21
order: 1
---

The base joiners join both two streams of JSON objects.
 
### StreamPreferredJoiner
The StreamPreferredJoiner is a joiner that allow us join both two streams of JSON objects, favoring the stream's fields.

Suppose next query:

`SELECT * FROM STREAM inputA JOIN SELECT * FROM TABLE inputB USING myStreamPreferredJoiner INSERT INTO STREAM output`

If we receive next message from `inputB` Kafka topic:
```json
{
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-Y"
}
```

And then we receive next message from `inputA` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-X": "VALUE-X"
}
```
We will receive next message from `output` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-X"
}
```
### TablePreferredJoiner
The TablePreferredJoiner is a joiner that allow us join both two streams of JSON objects, favoring the table's fields.

Suppose next query:

`SELECT * FROM STREAM inputA JOIN SELECT * FROM TABLE inputB USING myTablePreferredJoiner INSERT INTO STREAM output`

If we receive next message from `inputB` Kafka topic:
```json
{
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-Y"
}
```

And then we receive next message from `inputA` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-X": "VALUE-X"
}
```
We will receive next message from `output` Kafka topic:
```json
{
 "FIELD-A": "VALUE-A",
 "FIELD-B": "VALUE-B",
 "FIELD-C": "VALUE-C",
 "FIELD-X": "VALUE-Y"
}
```

### Clarifications
If you are using JSON objects without common fields you can use any joiner.