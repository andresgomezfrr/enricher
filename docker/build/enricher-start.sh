#!/usr/bin/env bash
envsubst < /opt/enricher/config/config_env.json > /opt/enricher/config/config.json
/opt/enricher/bin/enricher-start.sh /opt/enricher/config/config.json