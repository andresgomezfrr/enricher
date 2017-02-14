#!/usr/bin/env bash
envsubst < /alloc/var/enricher/config/config_env.json > /alloc/var/enricher/config/config.json
/alloc/var/enricher/bin/enricher-start.sh /alloc/var/enricher/config/config.json