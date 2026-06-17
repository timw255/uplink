<p align="center">
  <img src="assets/icons/256x256.png" width="128" alt="Uplink" />
</p>

<h1 align="center">Uplink</h1>

Syncs storage backends (local filesystem, S3, Azure Blob, Backblaze B2) into [Aprimo](https://www.aprimo.com/).

> This is a community-supported sync daemon and is not officially maintained
> or endorsed by Aprimo. It is provided as a helpful utility for users working
> with Aprimo. See the [Aprimo Developers](https://developers.aprimo.com) site
> for official documentation and supported resources.
>
> Part of the [Power Tools for Aprimo](https://aprimo.power-tools.app) collection of integration
> utilities. See [TRADEMARKS.md](./TRADEMARKS.md).

## Install

Pre-built binaries for Linux, macOS, and Windows (amd64 + arm64) are published on the [Releases page](https://github.com/timw255/uplink/releases). Each archive contains two files:

- `uplink` (or `uplink.exe` on Windows) — the daemon binary
- `uplink.yaml` — a starter config with placeholder credentials

Download, extract, edit `uplink.yaml` with your Aprimo `client_id` / `client_secret` / `environment`, and run `./uplink run`.

**Linux / macOS:**

```bash
# Replace vX.Y.Z with the latest version from the Releases page
curl -L -o uplink.tar.gz \
  https://github.com/timw255/uplink/releases/latest/download/uplink-vX.Y.Z-linux-amd64.tar.gz
tar xzf uplink.tar.gz
# Edit uplink.yaml — fill in your Aprimo credentials
./uplink run
```

**Windows (PowerShell):**

```powershell
# Replace vX.Y.Z with the latest version from the Releases page
Invoke-WebRequest `
  -Uri https://github.com/timw255/uplink/releases/latest/download/uplink-vX.Y.Z-windows-amd64.zip `
  -OutFile uplink.zip
Expand-Archive uplink.zip
cd uplink
# Edit uplink.yaml — fill in your Aprimo credentials
.\uplink.exe run
```

Verify the download by running `sha256sum -c SHA256SUMS.txt` (Linux/macOS) or the equivalent against the `SHA256SUMS.txt` published with each release.

Building from source is also supported — clone the repo and `go build ./cmd/uplink`. Pure Go, no CGO required.

## Quickstart

You'll need:

- The `uplink` binary on your PATH (download the latest release for your OS, or have your admin hand you one)
- An Aprimo `client_id` and `client_secret` — ask your Aprimo administrator
- Your Aprimo environment name — the subdomain in `https://<env>.aprimo.com`

**1. Save this as `uplink.yaml`** next to the `uplink` binary, filling in your Aprimo environment name and credentials:

```yaml
storage:
  data_dir: ./data

connectors:
  - name: drop
    type: localfs
    config:
      root: ./incoming
      poll_interval: 2s

  - name: aprimo
    type: aprimo
    config:
      environment: "<your-aprimo-environment>"
      client_id: "<your-client-id>"
      client_secret: "<your-client-secret>"

channels:
  - name: drop-to-aprimo
    source: drop
    destination: aprimo
    trigger:
      event: OnCreate
```

**2. Start the daemon:**

```sh
mkdir incoming
uplink run
```

(`uplink run` picks up `uplink.yaml` from the binary's directory automatically. Pass `--config=PATH` to point it elsewhere.)

Drop a file into `./incoming` — within a couple of seconds it shows up in Aprimo as a new draft record. Stop with Ctrl+C; restart any time and it picks up where it left off.

### Keeping secrets out of the config file (optional)

If you'd rather not store the client secret in plain text, replace `client_id` / `client_secret` with the env-var indirections:

```yaml
client_id_env: APRIMO_CLIENT_ID
client_secret_env: APRIMO_CLIENT_SECRET
```

Then set them in your shell before launching:

```sh
export APRIMO_CLIENT_ID=...
export APRIMO_CLIENT_SECRET=...
```

On Windows PowerShell, use `$env:APRIMO_CLIENT_ID = "..."`.

## Connectors

Connectors come in two roles: **sources** (where files come from — storage backends like local filesystems and cloud object stores) and **destinations** (where files land — DAMs). A channel binds one source to one destination plus a trigger, and runs one-way: source → destination. The individual connectors are documented below.

Every connector follows the same credential pattern: any secret can be set **inline** in the YAML or via the matching `*_env` field, which resolves from an environment variable at startup. Env-var values win over inline when both are set. The pattern lets a tracked `configs/example.yaml` reference env vars while a gitignored `configs/local.yaml` carries actual secrets — or vice versa.

Duration fields like `poll_interval` and `http_timeout` are written as a sequence of decimal numbers each with a unit suffix, no whitespace. Valid units are `s`, `m`, `h` — seconds is the floor. Examples: `30s`, `2m`, `1h`, `2h30m`, `1.5h`. Sub-second values (`500ms`, `0.5s`) and larger units (`1d`, `1w`) are both rejected at config load.

### localfs (source)

Watches a directory on the host filesystem. Detects creates, updates (via mtime + size), and deletes between polls.

```yaml
- name: fs-in
  type: localfs
  config:
    root: "./incoming"        # directory to watch (relative or absolute)
    poll_interval: "2s"
```

No credentials. The connector treats the configured `root` as its sandbox — it never reads outside it, even if a metadata-driven path tries to traverse with `..`.

### s3 (source)

Amazon S3 and any S3-compatible service (Cloudflare R2, Backblaze B2's S3 API, self-hosted object stores, etc).

```yaml
- name: s3-src
  type: s3
  config:
    region: us-east-1
    bucket: my-bucket
    prefix: "media/"          # optional; scope to a key prefix
    access_key: "AKIA..."     # OR access_key_env: AWS_ACCESS_KEY_ID
    secret_key: "..."         # OR secret_key_env: AWS_SECRET_ACCESS_KEY
    endpoint: "https://..."   # optional; for non-AWS S3 services
    use_path_style: false     # true for self-hosted S3 services that require it
    poll_interval: "60s"
```

Leave `access_key` / `secret_key` (and their `*_env` variants) all empty to use the SDK's ambient credential chain — instance profile on EC2/EKS, `~/.aws/credentials`, or `AWS_*` env vars. Identity uses ETag, so updates are detected whenever the object's bytes change.

### azblob (source)

Azure Blob Storage. Supports shared-key, SAS token, and connection-string auth.

```yaml
- name: az-src
  type: azblob
  config:
    account: mystorageacct
    container: my-container
    prefix: "media/"          # optional
    # Pick ONE of these three auth modes:
    account_key: "..."        # OR account_key_env: AZ_KEY
    # sas_token: "?sv=..."    # OR sas_token_env: AZ_SAS
    # connection_string: ".." # OR connection_string_env: AZ_CONN
    service_url: ""           # optional override; for sovereign clouds
    poll_interval: "60s"
```

Leave all three auth fields empty to use the ambient `DefaultAzureCredential` (managed identity, Azure CLI, env-var chain, etc).

### b2 (source)

Backblaze B2 using B2's native API (not the S3-compatible alias).

```yaml
- name: b2-src
  type: b2
  config:
    bucket: my-bucket
    prefix: "media/"          # optional
    key_id: "00..."           # OR key_id_env: B2_KEY_ID
    application_key: "K00..." # OR application_key_env: B2_APP_KEY
    poll_interval: "60s"
```

A read-only application key (capabilities `readFiles` / `listFiles` / `readBuckets`) is sufficient — Uplink only reads from B2.

### Polling multiple subtrees at different cadences

Every source connector accepts an optional `watchers:` block that splits its tree into per-prefix poll loops, each with its own `poll_interval`. The use case: a hot subdirectory you want picked up within seconds while the rest of the tree is fine being scanned every hour or daily.

```yaml
- name: fs-in
  type: localfs
  config:
    root: "./incoming"
    poll_interval: "1h"            # the default (empty-prefix) watcher
    watchers:
      - prefix: "hot/"
        poll_interval: "10s"
      - prefix: "archive/"
        poll_interval: "24h"
```

How it partitions the tree:

- **Longest matching prefix wins.** A file at `incoming/hot/x.jpg` is owned by the 10s watcher, NOT the root watcher. A file at `incoming/archive/2024/y.bin` is owned by the 24h watcher.
- **Coverage is disjoint.** No file is ever scanned by two watchers. No double events.
- **State is partitioned by watcher** (`fs-in` and `fs-in#hot/` are separate scope keys in the state table), so each loop runs independently — a stuck slow scan can't block the fast one.
- **`sync_log` keys on the connector name, not the watcher.** A file moving from `hot/` to `archive/` shows up as an Update on the same Aprimo record — the watcher partition is an implementation detail, not a sync-log boundary.

Identical prefixes within one connector are a config error. Omitting `watchers:` keeps the existing single-watcher behavior; this is fully backwards-compatible.

Cost is bounded by `O(N_watcher × frequency)` summed across watchers, not `O(N_total × max_frequency)` — which is what makes "watch 1K hot files every 10s, watch 1M archived files once a day" actually performant.

This works identically for s3 / azblob / b2 — same `watchers:` block, prefixes interpreted against the connector's configured prefix (e.g., S3 `prefix: media/` + watcher `prefix: hot/` watches `media/hot/`).

### aprimo (destination)

The Aprimo DAM. Always the destination; never used as a source.

```yaml
- name: aprimo-prod
  type: aprimo
  config:
    environment: "your-subdomain"   # the <env> in https://<env>.aprimo.com
    client_id: "..."                # OR client_id_env: APRIMO_CLIENT_ID
    client_secret: "..."            # OR client_secret_env: APRIMO_CLIENT_SECRET
    default_status: "draft"         # draft | released | archived (default: draft)
    default_collection: ""          # optional collection id; new records get filed here
    default_language: "en-US"       # IETF culture tag — required when channels using
                                    # this connector declare companions that emit
                                    # localized fields. Must match one configured in
                                    # Aprimo's Admin → Languages. Companion-supplied
                                    # values that don't specify `language` go to this
                                    # default.
    refresh_interval: "1h"          # how often the prefetched catalogs (field defs,
                                    # languages, classifications, option items,
                                    # users, user groups) reload in the background.
                                    # Default 1h. Set to 0s to disable; restart picks
                                    # up new fields when disabled. Companion scripts
                                    # reference these catalogs by display name.
    rps: 15                         # sustained per-second request budget. Set this
                                    # to your tenant's licensed Aprimo RPS (default
                                    # allowance is on the order of 15; higher values
                                    # are licensable per environment). 0 = disabled,
                                    # SDK falls back to 429-retry only. See the
                                    # "Rate limiting" subsection below.
    max_concurrent: 32              # cap on in-flight Aprimo HTTP requests. Memory
                                    # + socket-pool safety net independent of `rps`.
                                    # 0 (default) = uncapped.
    http_timeout: "60s"             # SDK request timeout
    direct_upload: true             # upload file bytes straight to Aprimo's
                                    # Azure Blob storage via a SAS URL, off the
                                    # rate-limited API. Default true; see below.
```

Authenticates via the `client_credentials` OAuth flow.

#### Direct-to-blob uploads

Aprimo's storage is Azure Blob, and its upload API can hand back a short-lived, writable **SAS URL** pointing straight at the backing blob. With `direct_upload: true` (the default), Uplink uploads file bytes **directly to that blob URL** — using the same block-parallel protocol AzCopy uses, via the Azure SDK already in the binary (no external tools) — instead of streaming them through Aprimo's HTTP upload service.

Two things make this a big win for upload-heavy work:

- **It bypasses the rate limiter.** The bytes never hit the Aprimo API, so a large file costs **one** request against your `rps` budget (to mint the SAS) plus the record write — not dozens of segment POSTs. Your whole RPS budget is freed for metadata, and the adaptive worker pool fills your bandwidth instead of pacing against the token bucket.
- **Azure ingests at full line rate.** Throughput is bounded by your pipe, not Aprimo's proxy.

Set `direct_upload: false` to route uploads through Aprimo's segmented upload service instead — a kill-switch for tenants where the direct path misbehaves. Either way, an interrupted upload re-streams the file on the next attempt and the record is created exactly once — see [If the daemon crashes mid-sync](#if-the-daemon-crashes-mid-sync).

At startup (and again on every `refresh_interval` tick) the connector prefetches the tenant's field-definition, language, classification, option-item, user, and user-group catalogs so companion scripts can reference everything by display name without ever touching opaque GUIDs. New fields added in Aprimo become visible at the next refresh; renamed fields likewise. If `refresh_interval: 0s`, restart the daemon to pick up changes.

#### Rate limiting

Aprimo enforces a per-tenant **token bucket** on its REST API:

- A **sustained rate** measured in requests per second (RPS). The default Aprimo allowance is on the order of 15 RPS per environment; higher values are licensable.
- A **burst buffer** of up to 100 requests. When the bucket is full (e.g., after a quiet period), the next 100 requests fire instantly. After that, requests pace at the sustained RPS until the bucket refills.
- Requests over both limits return **HTTP 429 Too Many Requests**.

The `rps` knob on the Aprimo connector mirrors this model client-side: Uplink installs a local token bucket with your configured RPS and the same 100-token burst. Every outbound API call (uploads, record creates, catalog fetches) acquires one token before firing, so the daemon never tries to drive Aprimo faster than the server is willing to accept. Result: **no 429s under normal load** — the retry path is reserved for genuinely unexpected throttling (other clients of the same tenant, maintenance windows, etc.).

Tune `rps` to match your tenant's licensed value exactly:

| `rps` setting | Effect |
|---|---|
| Equal to licensed RPS | Optimal — daemon paces itself perfectly, zero 429s. |
| Lower than licensed | Daemon under-utilizes; throughput capped below what your license allows. |
| Higher than licensed | Daemon overshoots; Aprimo returns 429s; SDK retries with backoff. Throughput throttled by server, just less gracefully. |
| `0` (default) | No client-side pacing. SDK relies on 429-retry alone — fine for low workloads, painful at scale. |

The `max_concurrent` knob is a separate concern: it caps how many HTTP requests are *in flight at once* (independent of their rate). Useful as a memory + socket-pool safety net when you have many workers × parallel segments. Setting `max_concurrent: 32` is reasonable for production; the rate limiter does most of the heavy lifting and `max_concurrent` keeps the in-flight set bounded.

### Channels

A channel binds one source to one destination with a trigger:

```yaml
channels:
  - name: fs-to-aprimo
    source: fs-in
    destination: aprimo-prod
    trigger:
      event: OnCreate                 # single event kind, OR
      # events: [OnCreate, OnUpdate]  # list of event kinds
      # filter: 'size > 0'            # optional filter expression
```

Use `event` for a single kind or `events` for a list — exactly one of the two must be set. A multi-event channel keeps a single sync_log stream per source path, so a file's Update lands as a new version on the same Aprimo record as its Create. Splitting Create and Update across separate channels gives each its own history and produces duplicate records on Update, which is rarely what you want.

Available kinds: `OnCreate`, `OnUpdate`, `OnDelete`. Source-side deletes are not propagated to Aprimo by design — an Aprimo record outliving its source file is the intended behavior. (There is intentionally no "move" kind: the supported backends have no rename primitive and no durable object id, so a relocation is a delete of the old path plus a create of the new one — see the note under [Enrich scripts](#enrich-scripts-metadata-from-the-asset-itself).)

Filter expressions use Google's [Common Expression Language](https://github.com/google/cel-spec) — the operators you'd expect are available: `==`, `!=`, `<`, `>`, `&&`, `||`, plus string helpers like `startsWith`, `endsWith`, `contains`, and the `in` membership test.

### Companion files

By default a synced asset lands in Aprimo with just its filename and content — `default_status: draft` and whatever the upload itself gives Aprimo. Most teams want more: the photographer, the campaign, the shot date, rights info, technical specs from the file's own headers. Uplink lets you attach **sandboxed Lua scripts** to a channel that react to *companion files* — XMP sidecars, JSON descriptors, per-language caption files — sitting next to the asset in the same directory. Each script returns a list of field entries that get folded into the parent asset's Aprimo record.

```yaml
channels:
  - name: photos
    source: fs-in
    destination: aprimo-prod
    trigger:
      event: OnCreate
    companions:
      - pattern: "${basename}.xmp"
        script: scripts/xmp.lua
      - pattern: "${basename}.caption.${lang}.txt"
        script: scripts/captions.lua
      - pattern: "${basename}.${extension}.metadata.*.json"
        script: scripts/metadata.lua
```

Script paths are resolved relative to the directory holding the `uplink` binary — the same directory the default `uplink.yaml` lives in. Use absolute paths if you keep scripts elsewhere.

#### Pattern grammar

The `pattern` field is matched against the filename portion of every entry in the source. Patterns operate within one directory — companions live next to the asset they describe. Tokens:

| Token | Matches |
|---|---|
| `${basename}` | The asset's path with its final extension stripped. `photos/sunset.jpg` → `photos/sunset`. **Required** in every pattern — the dispatcher uses it to recover the parent asset's `sync_log` row. |
| `${extension}` | The asset's final extension, no dot (e.g. `jpg`). One path segment, no internal dots. |
| `${name}` | A user-defined named capture (letters / digits / `_`). One segment, no internal dots. The captured value is available to the script as `uplink.match.vars.name`. |
| `*` | Anonymous wildcard. One segment, no internal dots. Captured positionally; available as `uplink.match.wildcards[1]`, `[2]`, etc. |
| anything else | Literal — including `.`. |

A pattern is matched only on filenames within the asset's directory; it cannot cross directory boundaries (a literal `/` is rejected at config load).

#### Lifecycle

A companion file never becomes its own Aprimo record. What happens depends on whether the parent asset exists yet:

- **Companion arrives before the asset.** The companion's own source event is dropped at dispatch (no parent row in `sync_log` to PATCH). When the asset arrives, the Create job runs a **presync** sweep: it lists the asset's directory, finds every file matching one of the channel's companion patterns, runs each script, and folds the returned fields into the Aprimo `Create` call. One API call instead of one Create + N PATCHes.
- **Companion arrives after the asset.** The dispatcher locates the parent's `sync_log` row, enqueues a companion job. The worker reads the companion's bytes, runs the script, and PATCHes the returned fields onto the existing record.
- **Companion modified.** Same path as "after." Each change PATCHes the latest field values onto the parent's record.
- **Companion deleted.** The script is invoked with `uplink.file.deleted = true` and `uplink.file.content = nil`. The script decides whether to PATCH (e.g., emit an empty value to clear a locale) or return `{}` (no-op). **The Aprimo record itself is never deleted by a companion event** — that's by design, same as source-side deletes of assets.
- **Asset content updated.** No companion presync re-runs. The existing record's metadata is left alone; companions PATCH separately when they themselves change. If you want a new content version on the same record without re-deriving metadata, change the asset; if you want updated metadata, touch the companion.

#### The contract a script honors

**Return a list of `{ name = "...", value = ... }` entries** (optionally with `language = "<culture>"`). `name` matches the field's display name in Aprimo's admin UI; case + surrounding whitespace are normalized. `language` is an IETF culture tag like `"en-US"` / `"fr-FR"`; when omitted, the value goes to the connector's `default_language`. Return `{}` to contribute nothing. The Aprimo connector looks at each field's data type, resolves names (classifications, option items, users, user groups, languages) to their internal IDs, formats the value to whatever wire shape Aprimo requires for that type, and consolidates same-field-different-language entries into one localized write. You never write `fieldId`, `languageId`, `addOrUpdate`, or `localizedValues` in the script.

#### Supported field types

Every Aprimo field type works — the resolver coerces the script's `value` into the right shape per type.

| Aprimo type | What the script returns | Resolver behavior |
|---|---|---|
| SingleLineText, MultiLineText, Html, RichContent | string | passes through |
| Numeric | number (Lua int or float) | formatted as decimal with period separator |
| Date | `"yyyy-MM-dd"` string | passes through |
| DateTime, Time, Duration | ISO-format string | passes through |
| Json | Lua table or JSON string | tables stringified; strings validated |
| TextList | list of strings | passes through |
| ClassificationList | classification path (`"Topics/Sports/Football"` or `"Topics > Sports > Football"`) | resolved to classification ID |
| OptionList | option-item name | resolved to item ID |
| UserList | user email or login name | resolved to user ID |
| UserGroupList | group name | resolved to group ID |
| LanguageList | culture tag or display name | resolved to language ID |
| RecordList | record ID | passes through |
| RecordLink | record ID | passes through |
| HyperlinkList | list of `{url = "...", label = "..."}` tables | passes through |

Single-value list types accept either a scalar (one entry) or a list (multiple) — `value = "Marketing"` and `value = {"Marketing"}` are equivalent for an OptionList. Same-field-different-language entries collapse into one Aprimo write carrying multiple localized values.

> **The catalogs refresh in the background.** By default the Aprimo connector reloads field definitions, languages, classifications, option items, users, and user groups every hour (`refresh_interval`). New fields appear without a daemon restart. Set `refresh_interval: 0` to disable.

#### The sandbox

Scripts run in a `gopher-lua` interpreter with a curated `uplink.*` global table. The dangerous stdlib (`os`, `io`, `debug`, `package`, `require`, `loadfile`, `dofile`, `load`, `loadstring`, `print`) is structurally absent — there's no filesystem access, no network, no process execution outside what `uplink.*` exposes. Each execution runs in a fresh Lua state with a 5-second wall-clock timeout and a 64 MiB memory ceiling.

The script sees exactly the file the engine triggered it on — there is **no** `read_file` or `list_files`. A script that needs to consult another file is signalling that the declaration model is wrong: add a second companion entry with its own pattern and script.

| Name | Shape |
|---|---|
| `uplink.asset` | `{ path, size, hash, record_id, extension }` — the parent asset, read-only. `record_id` is empty during presync (before the record is created); populated for ongoing PATCH jobs. |
| `uplink.file` | `{ path, content, deleted }`. `content` is the companion's bytes as a Lua string; `deleted` is true when the companion was deleted (and `content` is `nil`). |
| `uplink.match` | `{ pattern, basename, extension, vars, wildcards }`. `vars` is a table of named-capture values; `wildcards` is a positional list of `*` captures in pattern order. |
| `uplink.parse_json(s)` | Decodes JSON into a Lua table. |
| `uplink.parse_xml(s)` | Decodes XML (XMP-friendly: `desc["dc:rights"]` works directly). |
| `uplink.parse_csv(s, opts)` | Decodes CSV. `opts = { delimiter = ",", header_row = true, comment = "#" }`. |
| `uplink.log(level, msg, fields?)` | Structured log via the daemon's slog with `script` and `channel` defaults. |
| `uplink.fail(reason)` | Halt the script with an explicit error. |

#### Example: XMP sidecar from Photoshop / Lightroom

`hero.jpg` + `hero.xmp` dropped together. The XMP is the companion; the JPG is the asset.

```yaml
companions:
  - pattern: "${basename}.xmp"
    script: scripts/xmp.lua
```

```lua
-- scripts/xmp.lua
if uplink.file.deleted then return {} end

local xmp = uplink.parse_xml(uplink.file.content)
local desc = xmp["x:xmpmeta"]["rdf:RDF"]["rdf:Description"]

return {
  { name = "Rights Holder", value = desc["dc:rights"]  },
  { name = "Creator",       value = desc["dc:creator"] },
  { name = "IPTC Title",    value = desc["dc:title"]   },
}
```

The pattern `${basename}.xmp` matches `hero.xmp` next to `hero.jpg`, but also `report.xmp` next to `report.pdf` — the asset's own extension is irrelevant to this pattern. Use `${basename}.${extension}.xmp` (matching `hero.jpg.xmp`) if your tools emit the longer form.

#### Example: JSON descriptor from an export tool

```yaml
companions:
  - pattern: "${basename}.json"
    script: scripts/json-descriptor.lua
```

```lua
-- scripts/json-descriptor.lua
if uplink.file.deleted then return {} end

local meta = uplink.parse_json(uplink.file.content)
return {
  { name = "Client",    value = meta.client    },
  { name = "Campaign",  value = meta.campaign  },
  { name = "Shot Date", value = meta.shot_date },
  { name = "Spot Code", value = meta.spot_code },
}
```

#### Example: per-language caption files (named captures)

When the source of truth for each language is its own short text file next to the asset, encode the culture tag in the pattern with a named capture. One companion declaration handles every locale.

```
/incoming/hero.jpg
/incoming/hero.caption.en-US.txt    # "Sunset on Mt. Hood"
/incoming/hero.caption.fr-FR.txt    # "Coucher de soleil sur le Mt. Hood"
/incoming/hero.caption.ja-JP.txt    # "フッド山の夕日"
```

```yaml
companions:
  - pattern: "${basename}.caption.${lang}.txt"
    script: scripts/caption-per-lang.lua
```

```lua
-- scripts/caption-per-lang.lua
if uplink.file.deleted then
  -- Emit an empty value to clear the locale on the record.
  return {
    { name = "Caption", value = "", language = uplink.match.vars.lang },
  }
end

local text = uplink.file.content:match("^%s*(.-)%s*$")  -- trim
return {
  { name = "Caption", value = text, language = uplink.match.vars.lang },
}
```

Each of the three caption files fires independently. On Create the presync sweep matches all three at once and folds them into a single Aprimo call carrying three localized values for `Caption`. On later edits, each file PATCHes its own locale.

Patterns can hold multiple named captures. `${basename}.caption.${lang}.${region}.txt` matches `hero.caption.en.US.txt` and exposes `uplink.match.vars.lang` (`"en"`) and `uplink.match.vars.region` (`"US"`).

#### Example: JSON sidecar with multiple locales inside it

When one file ships every locale, a single match fires the script once and the script emits one entry per locale found in the body.

`/incoming/hero.jpg` + `/incoming/hero.json`:

```json
{
  "captions": {
    "en-US": "Sunset on Mt. Hood",
    "fr-FR": "Coucher de soleil sur le Mt. Hood",
    "ja-JP": "フッド山の夕日"
  }
}
```

```yaml
companions:
  - pattern: "${basename}.json"
    script: scripts/captions-bundle.lua
```

```lua
-- scripts/captions-bundle.lua
if uplink.file.deleted then return {} end

local meta = uplink.parse_json(uplink.file.content)
local out = {}
for culture, text in pairs(meta.captions or {}) do
  table.insert(out, { name = "Caption", value = text, language = culture })
end
return out
```

Same `name` × different `language` entries consolidate into one Aprimo write carrying all the locales — no need to repeat the field name across multiple records.

The contrast with the previous example: per-language files give you one-write-per-locale (each file PATCHes independently when it changes); a JSON bundle gives you one-write-per-edit (a single change rewrites every locale). Pick the shape that matches how the upstream tool emits the data.

Scripts are compiled at daemon startup; a syntactically broken script fails the daemon's startup so a bad change never silently corrupts metadata. Edits require a daemon restart.

### Enrich scripts (metadata from the asset itself)

Companion scripts react to a *sidecar file*. But often the metadata you want is already encoded in the asset's own path — a file synced from `emea/spring-2026/hero-banner.png` carries its region and campaign right there in the directory structure — with no sidecar to read. **Enrich scripts** cover that case: a sandboxed Lua script attached to a channel that runs against the asset *itself*, with no companion file and no pattern, once per lifecycle event.

```yaml
channels:
  - name: brand-assets
    source: fs-in
    destination: aprimo-prod
    trigger:
      events: [OnCreate, OnUpdate, OnDelete]
    enrich:
      - script: scripts/derive-from-path.lua
```

Each entry is just a `script:` path (resolved relative to the `uplink` binary, same as companions) — there is no `pattern`, because an enrich script isn't matched against a filename. A channel can declare multiple enrich scripts and mix them freely with `companions:`; their returned fields all fold into the same write.

#### When enrich scripts run

Enrich scripts run on the asset's own events, **honoring the channel's `trigger`** — so list the kinds you want under `events:`:

| Event | What happens | API call |
|---|---|---|
| **OnCreate** | Script runs; fields fold into the record `Create` — together with any companion presync fields. | folded into Create (no extra call) |
| **OnUpdate** | Content changed; script re-runs and fields fold into the same Update write. Path-derived tags stay correct. | folded into Update (no extra call) |
| **OnDelete** | Script runs with `uplink.event.deleted = true`; returned fields are **PATCHed** onto the existing record. The Aprimo record is **never deleted** — this is for flipping a field like `Lifecycle Status = "Retired"`. Dropped silently if the asset was never synced. | one PATCH |

A note on **moves / renames**: there is no `OnMove` trigger, by design. Uplink's scanners detect changes by diffing the backend listing *by path*, and the supported object stores (S3, Azure Blob, B2) have no rename primitive and expose no durable object id that survives a copy — so a relocation is genuinely indistinguishable from a **delete of the old path plus a create of the new path**, and that is exactly how it's reported. The practical consequence: moving a file does **not** re-file its existing record. The new path mints a fresh record (with new-path-derived fields via OnCreate) while OnDelete fires against the record at the old path. If your intent was to re-classify or re-collection an existing asset, **don't do it by moving files** — the identity isn't carried across. Treat the source tree's layout as fixed once an asset is synced, and drive re-classification from a companion or enrich *field* instead.

#### The script contract

Identical to companion scripts: **return a list of `{ name = "...", value = ... }` entries** (optionally `language = "<culture>"`), and the Aprimo connector resolves names to ids and coerces values per field type. Return `{}` to contribute nothing. See [Supported field types](#supported-field-types) for the full table — a `TextList` field takes a Lua list of strings, which is exactly what path segments give you.

The sandbox is the same gopher-lua environment as companion scripts (same library subset, same memory/time caps, same absent stdlib). Only the `uplink` table differs — there is **no** `uplink.file` and **no** `uplink.match` (an enrich script has no companion file and no captures):

| Name | Shape |
|---|---|
| `uplink.asset` | `{ path, size, hash, extension, record_id }` — read-only. `record_id` is empty during a Create (the record doesn't exist yet) and populated on Update / Delete. |
| `uplink.event` | `{ kind, deleted }`. `kind` is `"OnCreate"` / `"OnUpdate"` / `"OnDelete"`; `deleted` is a convenience bool, true exactly on delete. |
| `uplink.log` / `uplink.fail` | Same as companion scripts. |
| `uplink.parse_json` / `parse_xml` / `parse_csv` | Same pure parsers as companion scripts. |

#### Example: path segments as fields

A team files brand assets as `<region>/<campaign>/<filename>` — so `emea/spring-2026/hero-banner.png` should land in Aprimo with `Region = "emea"` and `Campaign = "spring-2026"`. When the source file is later removed, the record should be flagged rather than deleted. One enrich script does all of it:

```lua
-- A user-supplied script attached via `enrich:`. Not shipped with
-- Uplink — keep it wherever your other scripts live.
if uplink.event.deleted then
  -- The source file is gone. We never delete the Aprimo record; just
  -- flag it so downstream workflows can react.
  return { { name = "Lifecycle Status", value = "Retired" } }
end

-- Split the path into segments and drop the filename, leaving the
-- directories that describe the asset.
local segs = {}
for s in uplink.asset.path:gmatch("[^/]+") do
  table.insert(segs, s)
end
table.remove(segs)

-- This tree is two levels deep; bail out cleanly on anything shallower
-- so a stray top-level file doesn't write garbage.
if #segs < 2 then return {} end

return {
  { name = "Region",   value = segs[1] },   -- e.g. an OptionList
  { name = "Campaign", value = segs[2] },   -- e.g. a single-line text field
}
```

If you'd rather keep every segment without mapping positions, hand the whole list to a `TextList` field instead — `{ name = "Path Tags", value = segs }` writes one tag per directory. Either way the field names must match your Aprimo schema; the connector resolves and coerces from there.

### Ignoring files

Drop a `.uplinkignore` file at the root of a source connector to make matching paths **completely invisible to Uplink**. Uses gitignore-style syntax and works uniformly across localfs, s3, azblob, and b2 — each connector fetches the file via its native API at startup.

Ignored paths are dropped from `List` / `Walk` results and `Read` returns `ErrNotFound`. There is no scan event, no sync_log entry, no companion match — the daemon behaves as if the file simply isn't there. This is the right behavior for things you never want the system to see at all: lock files, partial downloads, working folders, OS detritus.

**Do not use `.uplinkignore` to hide companion files** like XMP sidecars or JSON descriptors. Companion declarations on a channel already prevent those files from becoming their own Aprimo records — adding them to `.uplinkignore` makes them invisible to the companion machinery too, so the metadata is silently lost. If a path matches both a companion pattern AND `.uplinkignore`, ignore wins: the file is invisible, never seen by any script.

Patterns are loaded once when the daemon starts; edit the file and restart to pick up changes. Only the root `.uplinkignore` is read — per-subdirectory ignore files are not discovered. Use recursive patterns (`**/*.tmp`, `**/build/`) in the root file to match nested paths.

An example covering common cases:

```gitignore
# OS metadata your designers never see and Aprimo doesn't want.
.DS_Store
Thumbs.db
desktop.ini

# Office and editor lock files written while a doc is open.
~$*.docx
~$*.xlsx
.~lock.*
*.swp
*~

# Partial downloads — these flicker into existence and would race
# the poll loop into uploading half a file.
*.crdownload
*.part
*.tmp

# Working folders the team uses for drafts and old versions but
# doesn't want mirrored to the DAM. Recursive — applies at any depth.
**/drafts/
**/_archive/
**/wip/
```

A line starting with `#` is a comment. A trailing `/` matches directories only. `**` matches any number of path segments — use it to apply a pattern at every depth.

## Tuning the engine

The `engine:` block at the top level of `uplink.yaml` controls the worker pool and the retry / backoff policy. All fields are optional; defaults work for typical workloads.

```yaml
engine:
  workers: 32             # OPTIONAL: pin a fixed pool, disabling auto-scaling (see below)
  poll_idle: "500ms"      # how long workers sleep when there's no claimable job
  max_attempts: 5         # max retries before a job lands in failed
  base_backoff: "2s"      # initial retry delay; doubles each attempt, capped at 5m
```

### Worker concurrency is automatic

You don't set a worker count for throughput. As long as a destination has `rps` configured (the normal case), the engine **auto-scales the number of concurrent jobs** to keep the Aprimo rate limiter saturated — the same adaptive pool the [bulk importer](#speed) uses. It watches how hard the limiter is actually being driven and grows the pool under a backlog, holds once it's running at the licensed `rps`, and shrinks back when the queue goes idle. A burst of big files ramps up to many concurrent uploads; a trickle of small ones settles low. The `rps` token bucket is a hard ceiling, so more workers can never exceed your tenant's rate — they only stop you from *under*-driving it.

Because the work is network I/O end to end (Stat + read + segmented upload + record write — almost no CPU), the pool is **not** bounded by core count. Its ceiling is a memory/socket safety bound, taken from the destination's `max_concurrent` (or 64 when that's uncapped). That's the number to raise if you want to let it run wider.

When to set each knob:

- **`workers`** (optional): set it to **pin a fixed pool** of exactly this many concurrent jobs and turn auto-scaling **off** — useful when you want deterministic, predictable concurrency instead of letting the engine size itself. Leave it unset for the auto-scaling default. (If **no** destination has `rps` configured there's no rate signal to scale against, so the engine runs a fixed pool of `workers` jobs, default 4, either way.)

- **`poll_idle`** (default 500ms): how long a worker waits between `ClaimNextJob` attempts when nothing's ready. Lower = more responsive on the first event after an idle period, higher = less SQLite traffic. 500ms is fine for almost everything. Drop to 100ms only if you measurably need faster pickup.

- **`max_attempts`** (default 5): how many times the engine retries a failing job before it lands in the `failed` queue (where it sits until an operator runs `uplink retry`). Permanent failures (e.g., a 403 from Aprimo) will burn through all five quickly anyway thanks to the exponential backoff. Bumping this rarely helps; it just delays the operator-visible failure.

- **`base_backoff`** (default 2s): the first retry delay. The engine doubles it each attempt, capped at 5 minutes. With defaults: 2s, 4s, 8s, 16s, 32s, then capped. Increase for backends with slow recovery from transient errors; rarely needed.

The engine block is optional — omit it and you get the defaults above.

## Subcommands

The daemon is the default subcommand:

```sh
uplink run
```

With no `--config`, the daemon loads `uplink.yaml` from the binary's directory.

The other subcommands operate on the same data directory the daemon uses. SQLite WAL mode means they're safe to run while the daemon is up.

```sh
uplink status   --data-dir=./data                          # job + sync_log summary
uplink retry    --id=ID | --channel=N | --all              # re-enqueue failed jobs
uplink inspect  sync   --path=P --channel=C                # latest sync_log row for a key
uplink inspect  state  --connector=N                       # connector state blob
uplink inspect  upload --job=ID                            # in-flight upload marker
uplink import   --file=M.jsonl [--dry-run]                 # bulk-load a JSONL manifest into Aprimo
uplink archive  --older-than=DUR [--out=DIR | --discard]   # prune old sync_log rows
uplink version
```

`uplink help` prints the same list at the CLI.

## If the daemon crashes mid-sync

Restart it. Each in-flight upload has a marker file in `data/uploads/`; the next claim picks up from where it left off — committed segments are not re-sent and records are not duplicated. `uplink inspect upload --job=<id>` prints any marker that's still in flight.


## Bulk import

The daemon is built for the steady state — files landing in a watched backend, one at a time, forever. A bulk import is the opposite shape: a one-shot batch you want in Aprimo now. Maybe you're migrating a pile of assets with an export of their metadata; maybe you're uploading 200k new files in one go, or stamping metadata across thousands of records that already exist. Same path either way. `uplink import` reads a JSONL manifest (one record per line) and, for each line, either creates a new record from an uploaded file, attaches a new file version to an existing record, or patches metadata onto an existing record — your choice, per line.

The import command stores state in a per-manifest ledger under the data dir recording which records are already done, so a re-run never uploads the same file twice. See [The ledger](#the-ledger-resume-and-dedup) below.

### The manifest

One JSON object per line (JSONL). Four keys are recognized:

| Key | Meaning |
|---|---|
| `id` | An existing Aprimo record id. Present → the line targets that record. |
| `file` | The asset's path **within the `--source` connector**. Present → its bytes are uploaded. |
| `status` | Optional lifecycle status: `draft`, `released`, or `archived`. |
| `fields` | The metadata: a list of `{ "name": ..., "value": ..., "language": ... }` entries — the **same contract** companion and enrich scripts use (see [The contract a script honors](#the-contract-a-script-honors)). `name` is the field's display name in Aprimo; `language` is optional and defaults to the connector's `default_language`. |

What each combination does:

| `id` | `file` | Action |
|:---:|:---:|---|
| —   | yes | **Create** a new record from the uploaded file, applying `fields`. |
| yes | yes | **Update**: upload the file as a new version on that record, applying `fields`. |
| yes | —   | **Metadata** only: PATCH `fields` (and/or `status`) onto the existing record — no upload. |
| —   | —   | Error — a line needs at least one of `id` or `file`. |

```json
{"file": "photos/sunset.jpg", "fields": [{"name": "Title", "value": "Sunset on Mt. Hood"}]}
{"id": "6892d77eb0d145a59a31b2be0127bc97", "file": "photos/hero.jpg", "status": "released"}
{"id": "cf91775e283644c798f4b2be0127bca2", "fields": [{"name": "Caption", "value": "Coucher de soleil", "language": "fr-FR"}]}
```

Unknown top-level keys are **rejected** by default — so a raw export with extra columns (`title`, `contentType`, `modifiedOn`, …) fails loudly, reminding you to move the ones you actually want into a `fields` array. Pass `--lenient` to ignore extra keys instead. Values inside `fields` are coerced per the field's Aprimo data type exactly as in a companion script: a `Numeric` field takes a JSON number, a `TextList` a JSON array of strings, a `ClassificationList` a path like `"Topics/Sports/Football"`, and so on — see [Supported field types](#supported-field-types).

### Dry run first

```sh
uplink import --file=records.jsonl --source=fs-in --dry-run
```

A dry run validates every line without writing anything: it confirms each line has an `id` or a `file`, that any `file` actually exists at its path under `--source`, and that every field resolves against the live Aprimo catalog (field names exist, classifications/option-items/users/languages resolve, values coerce). It still authenticates and prefetches the catalog — it just makes no changes. The command exits non-zero if any line fails validation, so it drops straight into an import script or CI gate. Fix the manifest, re-run until clean, then drop `--dry-run`.

Aprimo forbids `< > : " / \ | ? *` (and control characters) in a filename, so on upload Uplink replaces any of them with `_`. The dry run **flags every name it will rewrite** — logged as a warning (`old -> new`) and tallied in the summary as `filenames to be rewritten: N`. The line is still valid; this is just a heads-up so you can spot surprises — `a:b.jpg` and `a/b.jpg` both become `a_b.jpg` — before committing to a large import.

### Running it

```sh
uplink import --file=records.jsonl --source=fs-in --destination=aprimo-prod
```

- `--destination` names an `aprimo` connector from your config; the importer reads its credentials, `rps`, and `max_concurrent` straight from that connector's block. When the config defines exactly one aprimo connector, `--destination` can be omitted.
- `--source` names the connector the `file` paths are resolved against — any source connector (localfs, s3, azblob, b2). It's required only if some line carries a `file`; a metadata-only manifest doesn't need it.
- `--stop-on-error` aborts on the first failing line. The default processes the whole manifest and reports failures at the end — the usual bulk-import posture.

### The ledger (resume and dedup)

So every import keeps a ledger: one JSONL row per processed record (`{line, hash, action, dest_id, error?}`). By default it lives at:

```
<data_dir>/imports/<manifest>-<hash>.jsonl
```

keyed to the (manifest, destination) pair, so re-running the same import against the same Aprimo environment finds the same ledger. On a re-run, any record already recorded as `created`, `updated`, or `metadata` is **skipped** — that's the dedup that stops a crashed or re-issued import from uploading the same file multiple times. Failed and invalid lines are *not* marked done, so they're retried on the next run.

The ledger also records an intermediate `uploaded` state: the moment a file's bytes are in blob storage (with its token) but before the record is created. So if a crash lands in that window — the expensive part already done — **resume skips the re-upload and creates straight from the saved token.** If Aprimo has since swept the upload (it cleans unattached uploads up after a few days), the create simply fails with "upload token missing" and the pipeline re-uploads that one file and retries.

You normally don't touch any of this — just re-run the command and it picks up where it left off. `--restart` is the one knob: it ignores prior progress and re-processes every record from scratch.

A dry run keeps no ledger — it's pure validation with nothing worth persisting.

### Speed

A bulk import pushes against two limits at once: the **Aprimo API** (gated by your licensed `rps`) and your **bandwidth** (carrying the file bytes). The importer saturates both by running uploads and record-writes as decoupled stages with a queue between them — bytes stream straight to Azure blob storage [off the rate limiter](#direct-to-blob-uploads) while records get written at the `rps` pace. A slow multi-gigabyte upload runs in the background and never starves record creation, so you hit the API ceiling instead of stalling behind a transfer.

You normally set none of these: the upload stage auto-scales to fill your bandwidth and the create stage is paced by `rps`, so the system finds the right concurrency on its own. Reach for these only to **override the auto-tuning** — when you need a hard cap because the host is tight on memory or sockets, or you just want fixed, predictable concurrency instead of a pool that grows and shrinks:

- `--upload-concurrency=N` (default 32) — caps how many files upload at once. The auto-tuner never ramps past this; set it low to protect a constrained host, high to push a fat pipe harder. `--max-workers` is an alias.
- `--create-concurrency=N` (default 16) — caps concurrent record writes. The `rps` limiter already paces these, so this only matters if you want to hold record-writing below your licensed rate.

