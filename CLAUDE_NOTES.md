# CLAUDE_NOTES.md

Running notes for the `hermie` repo so I can catch myself up across sessions.
Newest context at the top of each section. **No secrets in this file.**

## What this repo is
Ansible project to **deploy & configure the NousResearch Hermes agent** on a
remote host, pointed at a local MLX LLM. It is *not* application code.

## Topology
- **LLM server:** the Mac I run on, `192.168.88.127:8080/v1` — `mlx_lm.server`
  (MLX), OpenAI-compatible, binds `0.0.0.0`. Serving
  `mlx-community/DeepSeek-R1-Distill-Qwen-7B-4bit`. Managed separately (see
  `~/reposdso/ai-serve`), **out of scope** for this repo.
- **Agent host:** `192.168.88.128`, Ubuntu 22.04 (NucBox5), user `pinky`
  (uid 1000, in `sudo` group). Hermes = github.com/NousResearch/hermes-agent at
  `~/.hermes/hermes-agent`. CLI symlink `~/.local/bin/hermes`. Config
  `~/.hermes/config.yaml` (mode 600); LLM target is the top-level `model:` block.

## Access to .128
- **SSH key auth is set up** — local `~/.ssh/id_ed25519` is in pinky's
  authorized_keys: `ssh pinky@192.168.88.128` works keyless.
- Password is in `hermie/.env` (gitignored, `USER:`/`PASS:` format). Used once
  via `expect` (`/usr/bin/expect`; `sshpass` is NOT installed) to seed the key.
- **sudo needs a password** (= the SSH password). For Ansible, pass it as a temp
  600 vars file, never on the CLI:
  ```bash
  cd ~/reposdso/hermie
  export PATH="/opt/homebrew/bin:$PATH"
  BP=$(grep '^PASS:' .env | cut -d: -f2-); umask 077
  printf 'ansible_become_password: "%s"\n' "$BP" > /tmp/hb.yml
  ansible-playbook playbook.yml -e @/tmp/hb.yml; rm -f /tmp/hb.yml
  ```

## What's deployed (verified working)
- **hermes role:** uv (user-level) → repo at pinned commit → venv → `uv sync` →
  CLI symlink → idempotent `model:` block patcher. `hermes -z "..."` round-trips
  to the LLM. `hermes_version` pinned to `a6e47314f...` (box ~111 commits behind
  upstream main — bump deliberately).
- **dashboard role:** `hermes dashboard` as a **user systemd service**
  `hermes-dashboard` (linger on → starts at boot), bound `0.0.0.0:9119` with
  `--insecure --tui`. Live at **http://192.168.88.128:9119** (HTTP 200 from the
  Mac). Web UI prebuilt to `hermes_cli/web_dist/` via `npm run build`.
  Manage: `systemctl --user {status,restart,stop} hermes-dashboard`.

- **mattermost role:** self-hosted Mattermost (Slack-like) via Docker Compose on
  `.128`. `mattermost/mattermost-team-edition:11.6.4` + `postgres:15-alpine`,
  project dir `~/mattermost`, bound `0.0.0.0:8065`. Live at
  **http://192.168.88.128:8065** (`/api/v4/system/ping` → 200 from the Mac).
  Bot accounts + personal access tokens enabled via env. DB password in
  `group_vars/hermes/vault.yml` (gitignored). Docker tasks run with `become`
  (pinky isn't in the `docker` group, but is in `sudo`). Manage on the box:
  `cd ~/mattermost && sudo docker compose {ps,restart,logs,down}`.

- **gateway role:** wires hermes' Mattermost channel + runs the messaging
  gateway as a service. Writes `MATTERMOST_URL`/`TOKEN`/`ALLOWED_USERS` into
  `~/.hermes/.env` (blockinfile, `no_log`; token from vault). Bot = **@hermie**,
  connects via WebSocket to `http://127.0.0.1:8065`. Service is hermes' own
  **`hermes-gateway`** user unit (enabled at boot, `Restart=always`). Idempotent
  (`changed=0`); reports "Mattermost CONNECTED". `mm_allowed_users: brian` in
  group_vars locks it to that MM user. Home channel (cron/autonomous output) =
  `MATTERMOST_HOME_CHANNEL` = Town Square id `bkkf739saif5tgzwaynhikgmho` (team
  CloudLogic). Channel IDs found via Mattermost API with the bot token:
  `curl -H "Authorization: Bearer $TOKEN" .../api/v4/teams/{id}/channels/name/town-square`.

- **homeassistant role:** ADOPTS the already-running Home Assistant container on
  `.128` (`ghcr.io/home-assistant/home-assistant:stable`, `network_mode: host`,
  `:8123`, config bind-mounted at `/opt/homeassistant/config`). Deliberately
  non-disruptive — the compose spec matches the running container so
  `docker compose up -d` finds it up-to-date and does NOT recreate it, and the HA
  config/db is never touched. Then wires hermes: writes `HASS_URL`/`HASS_TOKEN`
  to `~/.hermes/.env` (`no_log`) and enables the hermes `homeassistant` toolset
  so the agent can drive HA. Token = `hass_token` in vault (HA → Profile →
  Security → Long-Lived Access Tokens). Compose dir `~/homeass`.

- **weather role:** the daily forecast cron (see "Open / next") captured as a
  reproducible role.

## Cross-repo: astonks shares this stack
- The **astonks** project (`~/reposdso/astonks`) runs its own hourly "watcher"
  bot that **posts to this Mattermost** and **calls the same `.127:8080` MLX LLM**.
- It uses a SEPARATE bot, **@stonkbot** (user id `iz34tkszatrt7pi5j74kz3fu6r`),
  posting to a dedicated channel **#stonks** (`wx1br6bfat88xffn5johzhizmh`) in the
  CloudLogic team — distinct from @hermie / Town Square. Onboarding gotcha learned
  there: a MM bot must be added to the TEAM (System Console → User Management →
  Teams) BEFORE it can be `/invite`d to a channel.
- astonks pins model `Nous-Hermes-2-Mistral-7B-DPO-4bit-MLX` (cleaner prose than
  DeepSeek-R1 for summaries). All four MLX models are served on `.127:8080`.

## Gotchas learned (don't repeat)
- **hermes gateway: do NOT template your own systemd unit.** `hermes gateway run`
  REWRITES its own unit on startup (`--replace`), so a custom unit churns every
  play (unit "changed" → restart, forever). Instead drive the interactive
  installer non-interactively: `printf 'y\ny\n' | hermes gateway install`
  (two prompts: start-now, start-on-boot), guarded by a `stat` so it only runs
  when the unit is missing. The unit is named `hermes-gateway.service`.
- **hermes logs to `~/.hermes/logs/gateway.log`, NOT stdout/journald** — verify
  the Mattermost connection from that file (`grep 'mattermost connected'`), not
  `journalctl`.
- `hermes gateway install/start` are **interactive** — a bare Ansible `command`
  hangs forever waiting on the `[Y/n]` prompt (this hung a whole play once).
- `pkill -f "hermes gateway"` over SSH matches your own command and kills the
  session — kill by specific PID instead.
- **MATTERMOST_ALLOWED_USERS matches by Mattermost USER ID, not username.**
  Setting it to `brian` → "Unauthorized user: <id> (brian)" and silent drop.
  Use the user id (brian = `xbpkc5mq9jnk8mfngpjaygz5ay`). Same for other
  platforms per platform_registry.py ("comma-separated user IDs").
- **.128 has BROKEN IPv6 routing** (IPv4 works). DNS returns IPv6 first for
  GitHub / Docker Hub, so `git fetch` and `docker pull` fail intermittently with
  "network is unreachable" until Docker/git falls back to IPv4. Mitigations in
  place: hermes git task skips the fetch when already at the pinned SHA; the
  mattermost up task has `retries/until`. If a pull still flaps, just re-run.
- `changed_when: "'CHANGED' in stdout"` is WRONG — `CHANGED` is a substring of
  `UNCHANGED`. Use exact equality.
- `lineinfile` regexp `\.local/bin` was too broad and clobbered uv's
  `. "$HOME/.local/bin/env"` line in `.bashrc`. Use an exact `^...$` regexp.
- Dashboard web build output goes to `hermes_cli/web_dist/`, NOT `web/dist`.
- Ansible user-scope systemd needs `XDG_RUNTIME_DIR=/run/user/<uid>` (+ DBUS)
  in the task environment.
- node/npm/uv/hermes all live in `~/.local/bin` on .128.
- ⚠️ The dashboard exposes API keys on the LAN (why hermes requires `--insecure`).
  User accepted — trusted home LAN only; do NOT port-forward 9119 to the internet.

## Git state (as of 2026-06-05)
- `main` pushed to `origin` (`github.com:bbecomp99/hermie.git`), in sync. The
  mattermost/gateway/AI-server/weather work is all committed
  (`470f926`, `47f712b`, `cda623c`, `97e4f99`).
- Latest commit adds the **homeassistant role** + playbook wiring +
  `vault.yml.example` `hass_token` + README/notes refresh.
- No PR to anywhere; `main` is the working branch.

## Open / next
- **End-to-end chat CONFIRMED (2026-06-04):** DM to @hermie → gateway authorized
  brian → LLM call to DeepSeek-R1 (.127:8080, ~14s) → 398-char reply delivered
  back to Mattermost. The full path works.

- **DONE — daily weather cron to Town Square (built+verified 2026-06-05):**
  Job **`weather-daily`** (id `f9bea473519f`), schedule `0 7 * * *` = **7:00 AM
  Central** (box TZ confirmed **CDT**). Home channel = Town Square
  `bkkf739saif5tgzwaynhikgmho`. Cities: Dallas TX, Glen Haven WI, Billings MT,
  Miami FL — current + today + next 2 days.
  - **Architecture = script does EVERYTHING (`--no-agent`, no LLM).**
    `--script weather.py` (`~/.hermes/scripts/weather.py`, stdlib-only Python)
    fetches REAL data from **Open-Meteo** (no API key, hardcoded lat/long per
    city so tiny Glen Haven resolves right; WMO code→text + emoji map; °F),
    formats a polished Mattermost message (`##` header + per-city bold line +
    a 3-row markdown table Day/Forecast/Hi-Lo/Rain), and **posts it itself**
    via the Mattermost API (creds read from `~/.hermes/.env`:
    `MATTERMOST_URL`/`TOKEN`/`HOME_CHANNEL`).
  - **Why self-post instead of cron delivery:** cron's own delivery prepends a
    `Cronjob Response: <name> (job_id…)` header and appends a `To stop or manage
    this job…` footer — ugly. With `--no-agent`, **empty stdout = cron stays
    silent**, so the script posts directly and prints nothing → clean message,
    no wrapper. On post failure the script prints the message instead, so cron
    delivers it (wrapped) as a visible fallback rather than losing it.
    Test manually with `python3 ~/.hermes/scripts/weather.py --print` (formats
    only, no delivery) vs no args (live post).
  - Evolution this session: v1 agent-formatted (script stdout injected, LLM
    prettifies) worked but the LLM added greeting/sign-off chatter and the
    format drifted → switched to v2 deterministic self-posting script. The
    earlier note about "agent only formats" is superseded.
  - **Now captured as an Ansible role — reproducible.** `roles/weather`
    (tagged `weather`) templates the script (`weather.py.j2`, cities/coords/
    schedule in `roles/weather/defaults/main.yml`) and registers the cron job
    idempotently (guards on the job name in `hermes cron list`; uses argv form
    so the `0 7 * * *` schedule isn't split). Added after `gateway` in
    `playbook.yml` (the script needs the MATTERMOST_* that gateway writes to
    `.env`). Runs as the login user — **no become / no vault** needed. Apply
    just this slice: `ansible-playbook playbook.yml --tags weather`. Verified
    idempotent (`changed=0` on re-run). To change schedule/flags:
    `hermes cron remove <id>` on the box, then re-run the play.
  - ⚠️ **WHY not pure-LLM:** first attempt was a plain prompt telling Hermes to
    fetch weather via its `web` tool. The run logged **`tool_turns=0`** — the
    7B-4bit distill NEVER called a tool, it fabricated every temp and got the
    weekdays internally inconsistent. Small distilled reasoning models "think +
    answer" instead of tool-calling. So real data MUST be injected, not fetched
    by the model. (User picked pure-LLM first, then chose script-grounded after
    seeing the fabrication evidence.)
  - **Cron mechanics learned:** `hermes cron create '<sched>' '<prompt>'
    [--name N] [--deliver mattermost|platform:chat_id] [--script F] [--no-agent]`.
    `--script` files live under `~/.hermes/scripts/`; `.sh/.bash` run via bash,
    everything else via Python; default mode injects stdout into the prompt,
    `--no-agent` delivers stdout verbatim. Force a test run NOW with
    `hermes cron run <id>` then `hermes cron tick` (tick executes due jobs once).
    The **gateway** runs the scheduler (`hermes cron status` shows it). Delivery
    target `mattermost` (bare) = the configured `MATTERMOST_HOME_CHANNEL`.
  - **`hermes send`** (no LLM) is the easy way to post to MM from a script/CI:
    `hermes send --to mattermost "msg"` / `--list mattermost` shows targets.
  - Minor: box `.env` has a deprecated `TERMINAL_CWD=/home/pinky` (warns on each
    cron tick; harmless — move to `config.yaml terminal.cwd` someday).

- **.128 back UP (2026-06-05):** the earlier "down/powered-off" blocker cleared —
  ping 0% loss, keyless SSH works, both user units `active` (hermes-gateway,
  hermes-dashboard), MLX on .127:8080 serving. Hermes now `v0.15.1`, 238 commits
  behind upstream (still pinned deliberately).

- **ai-serve note (2026-06-05):** `serve.sh` MODEL line was switched to
  `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit` (lines 10-11 are now two
  identical lines — commented fallback == active, comment is redundant). But the
  server actually running serves DeepSeek-R1 (+ Qwens). `serve.log`/`serve.pid`
  are tracked but are runtime artifacts — consider gitignoring them.

- **Autonomy (requested, not built):** `hermes cron` / `hermes goals` (the
  gateway already runs the cron scheduler). The weather job above is the first
  concrete unattended task.
- Offered but not done: reverse-proxy/Tailscale if dashboard/Mattermost ever
  need real internet exposure (currently LAN-only).
