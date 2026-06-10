"""Host CPU/memory for a remote box via ONE lightweight SSH call — no agent.

Reads /proc/stat (twice, 1s apart, for a CPU% over that interval) and
/proc/meminfo in a single `ssh user@host '...'` exec. Pure stdlib + the openssh
client. CPU stats and meminfo in a container's /proc are namespaced, which is why
we read them on the target host over SSH instead of locally.
"""
import subprocess

REMOTE_CMD = (
    'grep "^cpu " /proc/stat; sleep 1; grep "^cpu " /proc/stat; '
    'echo __MEM__; grep -E "^MemTotal|^MemAvailable" /proc/meminfo'
)


def _cpu(line):
    # cpu  user nice system idle iowait irq softirq steal guest guest_nice
    p = [int(x) for x in line.split()[1:]]
    idle = p[3] + (p[4] if len(p) > 4 else 0)   # idle + iowait
    return idle, sum(p)


def parse(out):
    lines = out.strip().splitlines()
    cpu_lines = [l for l in lines if l.startswith("cpu ")]
    if len(cpu_lines) < 2:
        return None
    idle0, tot0 = _cpu(cpu_lines[0])
    idle1, tot1 = _cpu(cpu_lines[-1])
    dtot, didle = tot1 - tot0, idle1 - idle0
    cpu_pct = round(100 * (dtot - didle) / dtot, 1) if dtot > 0 else None

    mt = ma = None
    for l in lines:
        if l.startswith("MemTotal"):
            mt = int(l.split()[1])
        elif l.startswith("MemAvailable"):
            ma = int(l.split()[1])
    mem_pct = round(100 * (1 - ma / mt), 1) if mt and ma is not None else None
    return {
        "cpu_pct": cpu_pct,
        "mem_pct": mem_pct,
        "mem_total_kb": mt,
        "mem_avail_kb": ma,
        "mem_used_kb": (mt - ma) if (mt and ma is not None) else None,
    }


def sample(user, host, key, known_hosts, timeout=12):
    """One SSH round-trip → {cpu_pct, mem_pct, ...} or None on any failure."""
    cmd = [
        "ssh", "-i", key,
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=6",
        "-o", "StrictHostKeyChecking=yes",
        "-o", f"UserKnownHostsFile={known_hosts}",
        f"{user}@{host}", REMOTE_CMD,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            return None
        return parse(r.stdout)
    except Exception:  # noqa: BLE001 - any failure → no sample
        return None
