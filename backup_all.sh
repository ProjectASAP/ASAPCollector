#!/bin/bash

# copy changes within submodules into respective patch folders

./backup_otel_client_patches.sh
./backup_otel_collector_contrib_patches.sh
./backup_otel_collector_patches.sh
./backup_otel_patches.sh
./backup_otel_proto_patches.sh
./backup_telegraf_patches.sh