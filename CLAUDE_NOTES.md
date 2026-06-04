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

## Git state
- `main` pushed to `origin` (`github.com:bbecomp99/hermie.git`) through the
  hermes+dashboard work (commit `a81881a`). **Uncommitted since then:** mattermost
  role, gateway role, hermes git-skip guard, mm retries, vault.yml.example, and
  notes updates. Commit/push when the user asks.

## Open / next
- **End-to-end chat CONFIRMED (2026-06-04):** DM to @hermie → gateway authorized
  brian → LLM call to DeepSeek-R1 (.127:8080, ~14s) → 398-char reply delivered
  back to Mattermost. The full path works.
- **Autonomy (requested, not built):** `hermes cron` / `hermes goals` (the
  gateway already runs the cron scheduler). Needs a concrete unattended task.
- Offered but not done: reverse-proxy/Tailscale if dashboard/Mattermost ever
  need real internet exposure (currently LAN-only).
