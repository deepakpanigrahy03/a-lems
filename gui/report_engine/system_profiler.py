"""
A-LEMS Report Engine — System Profiler
Detects and stores hardware/environment profile.
Eliminates "Unknown hardware" in reports forever.
"""

from __future__ import annotations
import os, platform, sqlite3, uuid, json, logging
from pathlib import Path
from datetime import datetime
from .models import SystemProfile, EnvType

log = logging.getLogger(__name__)


def _detect_cpu_model() -> str:
    """Read CPU model string from /proc/cpuinfo or platform."""
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":")[1].strip()
    except Exception:
        pass
    return platform.processor() or "Unknown CPU"


def _detect_cpu_freq_mhz() -> float:
    """Max CPU frequency in MHz."""
    try:
        import psutil
        freq = psutil.cpu_freq()
        if freq:
            return float(freq.max) if freq.max else float(freq.current)
    except Exception:
        pass
    try:
        p = Path("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq")
        if p.exists():
            return float(p.read_text().strip()) / 1000.0
    except Exception:
        pass
    return 0.0


def _detect_rapl_zones() -> list[str]:
    """List available RAPL power domains."""
    zones = []
    rapl_base = Path("/sys/class/powercap")
    if not rapl_base.exists():
        return ["RAPL not available"]
    try:
        for zone in sorted(rapl_base.iterdir()):
            name_file = zone / "name"
            if name_file.exists():
                zones.append(name_file.read_text().strip())
    except Exception as e:
        log.debug(f"RAPL zone detection: {e}")
    return zones if zones else ["unknown"]


def _detect_env() -> EnvType:
    if Path("/.dockerenv").exists():
        return EnvType.DOCKER
    try:
        with open("/proc/1/cgroup") as f:
            content = f.read()
        if "docker" in content or "lxc" in content:
            return EnvType.DOCKER
    except Exception:
        pass
    # Cloud metadata endpoints (best-effort)
    try:
        import urllib.request
        urllib.request.urlopen(
            "http://169.254.169.254/latest/meta-data/", timeout=0.3
        )
        return EnvType.CLOUD
    except Exception:
        pass
    # VM heuristic: check for hypervisor CPU flag
    try:
        with open("/proc/cpuinfo") as f:
            if "hypervisor" in f.read():
                return EnvType.VM
    except Exception:
        pass
    return EnvType.LOCAL


def _detect_gpu() -> str | None:
    try:
        import subprocess
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=3
        )
        if result.returncode == 0:
            return result.stdout.strip().split("\n")[0]
    except Exception:
        pass
    return None


def _read_tdp() -> float | None:
    """Attempt to read TDP from RAPL max energy constraint."""
    try:
        p = Path("/sys/class/powercap/intel-rapl:0/constraint_0_power_limit_uw")
        if p.exists():
            return float(p.read_text().strip()) / 1e6
    except Exception:
        pass
    return None


def _disk_gb() -> float | None:
    try:
        import shutil
        total, _, _ = shutil.disk_usage("/")
        return round(total / 1e9, 1)
    except Exception:
        return None


def collect_profile() -> SystemProfile:
    """Collect current system profile. Safe — never raises."""
    try:
        import psutil
        ram_gb = psutil.virtual_memory().total / 1e9
        cores_physical = psutil.cpu_count(logical=False) or 1
        cores_logical = psutil.cpu_count(logical=True) or 1
    except ImportError:
        ram_gb = 0.0
        cores_physical = os.cpu_count() or 1
        cores_logical = cores_physical

    return SystemProfile(
        profile_id=str(uuid.uuid4()),
        cpu_model=_detect_cpu_model(),
        cpu_cores_physical=cores_physical,
        cpu_cores_logical=cores_logical,
        cpu_freq_max_mhz=_detect_cpu_freq_mhz(),
        ram_gb=round(ram_gb, 1),
        env_type=_detect_env(),
        os_name=platform.system() + " " + platform.release(),
        kernel=platform.version(),
        rapl_zones=_detect_rapl_zones(),
        gpu_model=_detect_gpu(),
        thermal_tdp_w=_read_tdp(),
        disk_gb=_disk_gb(),
    )


# ── DB persistence ────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS system_profiles (
    profile_id       TEXT PRIMARY KEY,
    cpu_model        TEXT,
    cpu_cores_phys   INTEGER,
    cpu_cores_logical INTEGER,
    cpu_freq_max_mhz REAL,
    ram_gb           REAL,
    env_type         TEXT,
    os_name          TEXT,
    kernel           TEXT,
    rapl_zones_json  TEXT,
    gpu_model        TEXT,
    thermal_tdp_w    REAL,
    disk_gb          REAL,
    collected_at     TEXT
);
"""


def ensure_profile_table(conn: sqlite3.Connection) -> None:
    conn.execute(_CREATE_TABLE)
    conn.commit()


def save_profile(profile: SystemProfile, conn: sqlite3.Connection) -> None:
    ensure_profile_table(conn)
    conn.execute("""
        INSERT OR REPLACE INTO system_profiles VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        profile.profile_id,
        profile.cpu_model,
        profile.cpu_cores_physical,
        profile.cpu_cores_logical,
        profile.cpu_freq_max_mhz,
        profile.ram_gb,
        profile.env_type.value,
        profile.os_name,
        profile.kernel,
        json.dumps(profile.rapl_zones),
        profile.gpu_model,
        profile.thermal_tdp_w,
        profile.disk_gb,
        profile.collected_at.isoformat(),
    ))
    conn.commit()


def load_latest_profile(conn: sqlite3.Connection) -> SystemProfile | None:
    """Load the most recent profile from the DB."""
    ensure_profile_table(conn)
    row = conn.execute("""
        SELECT * FROM system_profiles
        ORDER BY collected_at DESC LIMIT 1
    """).fetchone()
    if not row:
        return None
    cols = [d[0] for d in conn.execute("SELECT * FROM system_profiles LIMIT 0").description]
    d = dict(zip(cols, row))
    return SystemProfile(
        profile_id=d["profile_id"],
        cpu_model=d["cpu_model"] or "Unknown",
        cpu_cores_physical=d["cpu_cores_phys"] or 1,
        cpu_cores_logical=d["cpu_cores_logical"] or 1,
        cpu_freq_max_mhz=d["cpu_freq_max_mhz"] or 0.0,
        ram_gb=d["ram_gb"] or 0.0,
        env_type=EnvType(d["env_type"] or "LOCAL"),
        os_name=d["os_name"] or "Unknown",
        kernel=d["kernel"] or "",
        rapl_zones=json.loads(d["rapl_zones_json"] or "[]"),
        gpu_model=d["gpu_model"],
        thermal_tdp_w=d["thermal_tdp_w"],
        disk_gb=d["disk_gb"],
        collected_at=datetime.fromisoformat(d["collected_at"]),
    )


def get_or_collect_profile(db_path: str | Path) -> SystemProfile:
    """
    Load from DB if available, otherwise collect fresh and store.
    Called at Streamlit startup — idempotent.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        profile = load_latest_profile(conn)
        if profile is None:
            log.info("No system profile found — collecting fresh profile")
            profile = collect_profile()
            save_profile(profile, conn)
        return profile
    finally:
        conn.close()
