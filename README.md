# hermie

Ansible project to **deploy and configure the [Hermes agent](https://github.com/NousResearch/hermes-agent)**
(NousResearch) on a remote host, pointed at a local OpenAI-compatible LLM.

## Topology

```
  Mac  192.168.88.127   MLX server (mlx_lm.server)  ──serves──▶  :8080/v1   (the LLM)
                                                                     ▲
                                                                     │ OpenAI HTTP
  Agent 192.168.88.128  Hermes agent CLI (~/.hermes) ──────calls────┘
                        + Mattermost (:8065) + Home Assistant (:8123), Docker
```

This repo manages **only the agent side** (`.128`). The LLM on the Mac is
already running and out of scope here.

> The MLX box (`.127`) is also used by a separate project (**astonks**): its
> hourly watcher posts to the `#stonks` Mattermost channel as bot **@stonkbot**
> and calls the same LLM. Out of scope for this repo, but it shares this
> Mattermost instance and the `.127:8080` model server.

## What the playbook does

On the agent host (Ubuntu, user `pinky`):

1. Installs OS prereqs (`git`, `curl`, `build-essential`) — *sudo*.
2. Installs `uv` (user-level).
3. Clones/updates `NousResearch/hermes-agent` → `~/.hermes/hermes-agent` at `hermes_version`.
4. Builds the venv (`uv venv`) and installs deps (`uv sync --extra all --locked`).
5. Symlinks the `hermes` CLI into `~/.local/bin` and ensures it's on `PATH`.
6. Idempotently writes the `model:` block of `~/.hermes/config.yaml` to point at
   the LLM (backs up the file, leaves all other settings/keys untouched).

## Roles (playbook order)

The playbook now does more than install the CLI. Roles run in this order:

| Role            | What it sets up |
|-----------------|-----------------|
| `hermes`        | The steps above — the agent CLI + `config.yaml` model block. |
| `dashboard`     | `hermes dashboard` as a user systemd service `hermes-dashboard`, bound `0.0.0.0:9119` (LAN-only; uses `--insecure`). |
| `mattermost`    | Self-hosted Mattermost (team edition) + Postgres via Docker Compose, `:8065`. |
| `gateway`       | Wires the hermes Mattermost channel and runs the messaging gateway user service `hermes-gateway` (bot **@hermie**). |
| `homeassistant` | Adopts the **existing** Home Assistant container on `.128` (non-disruptive — never touches the HA config/db), and enables the hermes `homeassistant` toolset so the agent can drive HA. |
| `weather`       | Daily forecast cron delivered to Mattermost (`hermes cron`, script-grounded). |
| `toolsets`      | Prunes the agent to a minimal per-platform toolset (the bot runs a small 3B that misfires when given too many tools). Idempotent; `--tags toolsets`. |
| `stockcheck`    | Registers a deterministic `check_stock` MCP tool: fetches the local stonks API, evaluates RVOL / alpha-vol / alpha-price stability, and posts a ✅ LIKE to Mattermost #stonks when a ticker qualifies. `--tags stockcheck`. |

So beyond the CLI, `.128` ends up running long-running services:
`hermes-dashboard` + `hermes-gateway` (user systemd) and the Mattermost + Home
Assistant containers. The bare `hermes` CLI itself is still just invoked on demand.

## Prerequisites

- **Control node (this Mac):** `brew install ansible`
- **Auth to `.128`:** an SSH key (recommended — `ssh-copy-id pinky@192.168.88.128`),
  or password auth via `sshpass` + `--ask-pass --ask-become-pass`.

## Usage

```bash
cp inventory/hosts.yml.example inventory/hosts.yml   # set host/user/auth
ansible hermes -m ping                               # confirm connectivity

ansible-playbook playbook.yml --check                # dry run
ansible-playbook playbook.yml                        # apply
# password auth instead of a key:
# ansible-playbook playbook.yml --ask-pass --ask-become-pass
```

## Settings

`group_vars/hermes/main.yml`:

| Variable               | Default                                            | Notes |
|------------------------|----------------------------------------------------|-------|
| `hermes_repo`          | `…/NousResearch/hermes-agent.git`                  | source repo |
| `hermes_version`       | `main`                                             | branch/tag/commit to deploy |
| `hermes_model_default` | `llama3.2:3b-hermes`                               | Ollama model tag (custom 64K-ctx variant; see note) |
| `hermes_model_provider`| `custom`                                           | provider type in config.yaml |
| `hermes_model_base_url`| `http://192.168.88.127:11434/v1`                   | the LLM endpoint (Ollama; MLX `:8080` retired for the agent) |

> **LLM:** the agent now talks to **Ollama** on the Mac (`:11434`), not MLX —
> `mlx_lm.server` can't emit structured `tool_calls`. `llama3.2:3b-hermes` is a
> custom Ollama model (`llama3.2:3b-instruct` + `num_ctx 65536`, since Hermes
> requires ≥64K context). Recreate it on the Mac with:
> `printf 'FROM llama3.2:3b-instruct-q4_K_M\nPARAMETER num_ctx 65536\n' | ollama create llama3.2:3b-hermes -f -`

Role toggles in `roles/hermes/defaults/main.yml`: `hermes_python_version`,
`hermes_install_extras`, `hermes_manage_config`.

### Secrets

Secrets live in `group_vars/hermes/vault.yml` (gitignored; see
`vault.yml.example` for the keys):

| Variable        | Used by | Where to get it |
|-----------------|---------|-----------------|
| `mm_db_password`| mattermost | you choose it (Postgres password) |
| `mm_bot_token`  | gateway | Mattermost → System Console → Integrations → Bot Accounts |
| `hass_token`    | homeassistant | Home Assistant → Profile → Security → Long-Lived Access Tokens |
