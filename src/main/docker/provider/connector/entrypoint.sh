#!/bin/bash
set -e

# Patch config file with configured settings
cd /app/conf

if [[ -e config-template.json ]]; then
  if [[ ! -e config.json || ${CONFIGURATION_FORCE_UPDATE,} == "true" ]]; then
    export IDS_PUBKEY_ID=$(cat /proc/sys/kernel/random/uuid)
    export IDS_ENDPOINT_ID=$(cat /proc/sys/kernel/random/uuid)
    export IDS_MODEL_ID=$(cat /proc/sys/kernel/random/uuid)
    envsubst < config-template.json > config.json
    rm -f config-template.json
  fi
fi

# Launch connector
cd /app

java "org.springframework.boot.loader.JarLauncher"
