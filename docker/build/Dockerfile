FROM java:openjdk-8-jre

MAINTAINER info@redborder.com

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /alloc/var/ /alloc/log/ks-enricher /app

ADD enricher-dist.tar.gz /alloc/var/
RUN rm -f /alloc/var/enricher/config/*

ADD log4j.xml /alloc/var/enricher/config/
ADD config_env.json /alloc/var/enricher/config/
ADD enricher-start.sh /app

CMD /app/enricher-start.sh


