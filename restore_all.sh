#!/bin/bash

# apply patches to respective submodules

./restore_otel_client_patches.sh
./restore_otel_collector_contrib_patches.sh
./restore_otel_collector_patches.sh
./restore_otel_patches.sh
./restore_otel_proto_patches.sh
./restore_telegraf_patches.sh