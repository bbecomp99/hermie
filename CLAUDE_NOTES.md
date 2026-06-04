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

## Gotchas learned (don't repeat)
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
- Branch **`ansible-hermes-deploy`**, commit `bcb7c48` (Ansible project). Not pushed.

## Open / next
- **Autonomy (requested, not built):** add an `autonomy` role for `hermes cron`
  / `hermes goals`. Waiting on the user for (a) a Telegram bot token so it can
  message them (BotFather; lock to their account) or another channel, and
  (b) what concrete unattended task to schedule.
- Offered but not done: push the branch; reverse-proxy/Tailscale if the
  dashboard ever needs real internet exposure.
