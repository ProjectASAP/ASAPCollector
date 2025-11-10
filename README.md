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
