# DataCollector

This repository vendors a full Telegraf checkout as a Git submodule under
`telegraf/` and carries the custom Gorilla aggregator/output directly inside
that tree so you can build everything in one place.

## Cloning

Clone with submodules on first checkout so the embedded Telegraf tree is pulled
automatically:

```bash
git clone --recurse-submodules git@github.com:ProjectASAP/DataCollector.git
# or
git clone --recurse-submodules https://github.com/ProjectASAP/DataCollector.git
```

If you have an existing clone, pull down the submodule once:

```bash
cd DataCollector
git submodule update --init --recursive
```

After cloning, work inside `telegraf/` to build (e.g. `make telegraf`) with the
custom Gorilla plugins already available under `telegraf/plugins/`.

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

3. When upstream Telegraf updates are needed, pull them into the submodule:

   ```bash
   git submodule update --remote telegraf
   ```
