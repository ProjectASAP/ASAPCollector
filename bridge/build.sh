#!/bin/bash

# script assumes we're in source directory
cd "$(dirname "$0")" || exit 1

mkdir bin 2> /dev/null
mkdir bin/otel 2> /dev/null
mkdir bin/telegraf 2> /dev/null

# make sure we have all needed tools installed
missing=()
if ! [ -x "$(command -v builder)" ]; then
    missing+=("go install go.opentelemetry.io/collector/cmd/builder@latest")
fi

if ! [ -x "$(command -v telemetrygen)" ]; then
    missing+=("go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest")
fi

if [[ "${#missing[@]}" -ne 0 ]]; then
    echo -e "You appear to be missing some needed packages. Please run the below command(s) then restart your shell:\n"
    for i in "${!missing[@]}"; do
        echo "${missing[i]}"
    done
    exit 1
fi

# build OTEL
echo "Building OTEL..."
builder --config configs/otel-build.yaml --verbose || {
    echo -e "\nCould not build OTEL, exiting..."
    exit 1
}
echo -e "\nSuccessfully built OTEL."

echo

# build Telegraf
echo "Building Telegraf..."
cd ../telegraf || exit 1
(make build_tools && ./tools/custom_builder/custom_builder --config-dir ../bridge/configs) || {
    echo -e "\nCould not build Telegraf, exiting..."
    exit 1
}
mv telegraf ../bridge/bin/telegraf
echo -e "Successfully built Telegraf."
cd ../bridge || exit 1
