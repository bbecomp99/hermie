# hermie

Ansible project to **deploy and configure the [Hermes agent](https://github.com/NousResearch/hermes-agent)**
(NousResearch) on a remote host, pointed at a local OpenAI-compatible LLM.

## Topology

```
  Mac  192.168.88.127   MLX server (mlx_lm.server)  ──serves──▶  :8080/v1   (the LLM)
                                                                     ▲
                                                                     │ OpenAI HTTP
  Agent 192.168.88.128  Hermes agent CLI (~/.hermes) ──────calls────┘
```

This repo manages **only the agent side** (`.128`). The LLM on the Mac is
already running and out of scope here.

## What the playbook does

On the agent host (Ubuntu, user `pinky`):

1. Installs OS prereqs (`git`, `curl`, `build-essential`) — *sudo*.
2. Installs `uv` (user-level).
3. Clones/updates `NousResearch/hermes-agent` → `~/.hermes/hermes-agent` at `hermes_version`.
4. Builds the venv (`uv venv`) and installs deps (`uv sync --extra all --locked`).
5. Symlinks the `hermes` CLI into `~/.local/bin` and ensures it's on `PATH`.
6. Idempotently writes the `model:` block of `~/.hermes/config.yaml` to point at
   the LLM (backs up the file, leaves all other settings/keys untouched).

It does **not** start a long-running service — hermes is a CLI you invoke
(`hermes`, `hermes setup`, `hermes gateway`, `hermes cron`, …).

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
| `hermes_model_default` | `mlx-community/DeepSeek-R1-Distill-Qwen-7B-4bit`   | model id served by the MLX box |
| `hermes_model_provider`| `custom`                                           | provider type in config.yaml |
| `hermes_model_base_url`| `http://192.168.88.127:8080/v1`                    | the LLM endpoint |

Role toggles in `roles/hermes/defaults/main.yml`: `hermes_python_version`,
`hermes_install_extras`, `hermes_manage_config`.
