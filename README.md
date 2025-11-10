# DataCollector

This repository includes custom Telegraf output plugins (see `telegraf-plugins/`)
and now vendors a full Telegraf checkout as a Git submodule under `telegraf/`
so you can build everything in one place.

## Cloning

Clone with submodules on first checkout so the embedded Telegraf tree is pulled
automatically:

```bash
git clone --recurse-submodules git@github.com:approx-telemetry/DataCollector.git
# or
git clone --recurse-submodules https://github.com/approx-telemetry/DataCollector.git
```

If you have an existing clone, pull down the submodule once:

```bash
cd DataCollector
git submodule update --init --recursive
```

After cloning, work inside `telegraf/` to build (e.g. `make telegraf`) while the
plugins remain in `telegraf-plugins/outputs/`.

## Working with the Telegraf submodule

1. Enter the vendored repo and install Go dependencies:

   ```bash
   cd telegraf
   go mod tidy
   ```

2. Build or test Telegraf just like the upstream project:

   ```bash
   make telegraf   # or `make test`
   ```

3. To use the local `gorilla_s3` plugin without copying files, ensure that
   `plugins/outputs/all/all.go` imports it and that `go.mod` has a `replace`
   rule pointing to `../telegraf-plugins/outputs/gorilla_s3`, then run:

   ```bash
   go get github.com/approx-telemetry/DataCollector/telegraf-plugins/outputs/gorilla_s3
   ```

4. When upstream Telegraf updates are needed, pull them into the submodule:

   ```bash
   git submodule update --remote telegraf
   ```
