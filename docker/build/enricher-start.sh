#!/usr/bin/env bash
envsubst < /opt/enricher/config/config_env.json > /opt/enricher/config/config.json
envsubst < /opt/enricher/config/log4j2_env.xml > /opt/enricher/config/log4j2.xml

exec /opt/enricher/bin/enricher-start.sh /opt/enricher/config/config.json
