[![CircleCI](https://circleci.com/gh/wizzie-io/enricher/tree/master.svg?style=shield&circle-token=51379c78ee81176b6fc502877428f5e4b0c83ac9)](https://circleci.com/gh/wizzie-io/enricher/tree/master)

# Enricher

Enricher is a stream enriching engine based of Kafka Streams. Enricher does joins (streams and tables) end enrichment with any data store system that you define. You only need to define a JSON stream where you specify the joiners and enrichers to use and your enrichment query.

Enricher offers us: scalability, fault tolerance, back-preassure and full Kafka integration ... [Try it now!!](https://wizzie-io.github.io/enricher/getting/base-tutorial.html)

## Documentation

You can find the docs on the [Docs Website](https://wizzie-io.github.io/enricher/)

## Getting Started

You can get started with Enricher with this [tutorial](https://wizzie-io.github.io/enricher/getting/base-tutorial.html).

## Compiling Sources

To build this project you can use `maven` tool. 

If you want to build the JAR of the project you can do:

```
mvn clean package
```

If you want to check if it passes all the test:

```
mvn test
```

If you want to build the distribution tar.gz:

```
mvn clean package -P dist
```

If you want to build the docker image, make sure that you have the docker service running:

```
mvn clean package -P docker
```

## Contributing

1. [Fork it](https://github.com/wizzie-io/enricher/fork)
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Create a new Pull Request
