# DataCollector

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
