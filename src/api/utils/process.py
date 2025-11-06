import os
import shlex
import subprocess
import threading
import time
import json
from pathlib import Path
import sys

_LOCK = threading.RLock()
_PROCESSES: dict[str, subprocess.Popen] = {}
_META: dict[str, dict] = {}

# Project paths
REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src"

PROCESS_REGISTRY_PATH = REPO_ROOT / "data" / "process_registry.json"
LEGACY_PROCESS_REGISTRY_PATH = SRC_ROOT / "data" / "process_registry.json"


def _load_registry() -> dict:
    # Prefer standard path; fall back to legacy src/data path
    for path in (PROCESS_REGISTRY_PATH, LEGACY_PROCESS_REGISTRY_PATH):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return {}


def _save_registry(reg: dict) -> None:
    # Best-effort write to both standard and legacy paths
    try:
        PROCESS_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(PROCESS_REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(reg, f)
    except Exception:
        pass
    try:
        LEGACY_PROCESS_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LEGACY_PROCESS_REGISTRY_PATH, "w", encoding="utf-8") as f:
            json.dump(reg, f)
    except Exception:
        pass


def clear_process_registry() -> None:
    """Clear all saved PIDs from the registry files."""
    try:
        _save_registry({})
    except Exception:
        pass


def _update_registry(provider_norm: str, pid: int, cmd: str) -> None:
    with _LOCK:
        reg = _load_registry()
        reg[provider_norm] = {
            "pid": pid,
            "cmd": cmd,
            "updated_at": time.time(),
        }
        _save_registry(reg)


def _remove_from_registry(provider_norm: str) -> None:
    with _LOCK:
        # Read strictly from the canonical registry to avoid wiping entries
        try:
            with open(PROCESS_REGISTRY_PATH, "r", encoding="utf-8") as f:
                reg = json.load(f)
            if not isinstance(reg, dict):
                return
        except Exception:
            # If canonical cannot be read, try legacy; if that also fails, abort
            try:
                with open(LEGACY_PROCESS_REGISTRY_PATH, "r", encoding="utf-8") as f:
                    reg = json.load(f)
                if not isinstance(reg, dict):
                    return
            except Exception:
                return
        if provider_norm in reg:
            reg.pop(provider_norm, None)
            _save_registry(reg)


def _get_registry_entry(provider_norm: str) -> dict | None:
    reg = _load_registry()
    entry = reg.get(provider_norm)
    return entry if isinstance(entry, dict) else None


def _get_registry_entry_canonical(provider_norm: str) -> dict | None:
    """Read provider entry strictly from canonical registry path.

    Does not fall back to legacy; aligns with stop API requirement.
    """
    try:
        with open(PROCESS_REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        entry = data.get(provider_norm)
        return entry if isinstance(entry, dict) else None
    except Exception:
        return None


def _stop_by_pid(pid: int, timeout: float = 10.0) -> dict:
    """Attempt to stop a process by PID. Uses Windows taskkill when available.

    Returns a dict with status and exit information when possible.
    """
    try:
        if os.name == "nt":
            # Try graceful termination first, then force, with subprocess timeouts
            try:
                r = subprocess.run(["taskkill", "/PID", str(pid), "/T"], capture_output=True, text=True, timeout=timeout)
                if r.returncode != 0:
                    # Force kill
                    r2 = subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True, timeout=timeout)
                    if r2.returncode == 0:
                        return {"status": "killed_by_pid", "pid": pid}
                    else:
                        return {"status": "not_running_pid", "pid": pid, "error": r2.stderr.strip()}
                else:
                    return {"status": "stopped_by_pid", "pid": pid}
            except subprocess.TimeoutExpired:
                return {"status": "timeout_pid", "pid": pid}
            except Exception as e:
                return {"status": "error_pid", "pid": pid, "error": str(e)}
        else:
            # POSIX: attempt TERM then KILL
            try:
                import signal
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
            # Best-effort wait
            try:
                time.sleep(min(1.0, timeout))
            except Exception:
                pass
            try:
                import signal
                os.kill(pid, signal.SIGKILL)
                return {"status": "killed_by_pid", "pid": pid}
            except Exception as e:
                return {"status": "error_pid", "pid": pid, "error": str(e)}
    except Exception as e:
        return {"status": "error_pid", "pid": pid, "error": str(e)}


def normalize_provider(provider: str) -> str:
    return provider.strip().lower()


def env_var_names_for_provider(provider: str) -> list[str]:
    norm = provider.upper().replace("-", "_")
    return [
        f"SCRAPER_CMD_{norm}",
        f"APP_SCRAPER_CMD_{norm}",
        "SCRAPER_CMD",
        "APP_SCRAPER_CMD",
    ]


def get_scraper_cmd_for_provider(provider: str) -> str | None:
    for name in env_var_names_for_provider(provider):
        val = os.environ.get(name)
        if val:
            return val
    return None


def default_cmd_for_provider(provider: str) -> str | None:
    """Return a sensible default command for known providers when env is not set.

    Uses module execution so imports resolve consistently with PYTHONPATH=src.
    """
    p = normalize_provider(provider)
    defaults: dict[str, str] = {
        # Tire Rack default: entry module orchestrates scraping and retries
        "tire-rack": "python -m src.providers.tire_rack.index",
        # Driver Right default: run the main scraper script directly as a module
        "driver-right": "python -m src.providers.driver_right.driver_right",
        # Custom Wheel Offset: run the main scraper module directly
        "custom-wheel-offset": "python -m src.providers.custom_wheel_offset.custom_wheel_offset",
    }
    return defaults.get(p)


def spawn_provider_process(provider: str, cmd_str: str) -> subprocess.Popen:
    try:
        argv = shlex.split(cmd_str, posix=False)
    except Exception:
        argv = cmd_str.split()

    if argv and ((argv[0].startswith('"') and argv[0].endswith('"')) or (argv[0].startswith("'") and argv[0].endswith("'"))):
        argv[0] = argv[0][1:-1]

    project_root = REPO_ROOT
    # Ensure provider processes can import from our src/ packages
    env = os.environ.copy()
    src_dir = SRC_ROOT
    existing_py_path = env.get("PYTHONPATH", "")
    if str(src_dir) not in existing_py_path.split(os.pathsep):
        env["PYTHONPATH"] = (
            f"{str(src_dir)}{os.pathsep}{existing_py_path}" if existing_py_path else str(src_dir)
        )
    try:
        proc = subprocess.Popen(argv, cwd=str(project_root), close_fds=False, env=env)
        # Record PID in registry
        _update_registry(provider, proc.pid, cmd_str)
        return proc
    except Exception:
        proc = subprocess.Popen(cmd_str, cwd=str(project_root), shell=True, env=env)
        _update_registry(provider, proc.pid, cmd_str)
        return proc


def start_provider(provider: str, cmd_override: str | None = None) -> dict:
    provider_norm = normalize_provider(provider)
    # Resolve command from override, environment, or registry fallback
    cmd_str = cmd_override or get_scraper_cmd_for_provider(provider_norm)
    if not cmd_str:
        entry = _get_registry_entry(provider_norm)
        if entry and isinstance(entry.get("cmd"), str) and entry["cmd"].strip():
            cmd_str = entry["cmd"].strip()
    # If still not resolved, try built-in defaults for known providers
    if not cmd_str:
        cmd_str = default_cmd_for_provider(provider_norm)
    else:
        # If a script path was configured but is missing, fall back to default module entry
        try:
            args = shlex.split(cmd_str, posix=False)
        except Exception:
            args = cmd_str.split()
        # Find any explicit .py script in the command and verify existence
        script_arg = next((a for a in args if a.lower().endswith('.py')), None)
        if script_arg:
            script_path = Path(script_arg)
            if not script_path.is_absolute():
                script_path = REPO_ROOT / script_path
            if not script_path.exists():
                fallback = default_cmd_for_provider(provider_norm)
                if fallback:
                    cmd_str = fallback
    if not cmd_str:
        raise RuntimeError(
            f"No command configured for provider '{provider_norm}'. "
            f"Set one of: {', '.join(env_var_names_for_provider(provider_norm))}"
        )

    with _LOCK:
        proc = _PROCESSES.get(provider_norm)
        if proc and proc.poll() is None:
            return {"status": "already_running", "provider": provider_norm, "pid": proc.pid}

        proc = spawn_provider_process(provider_norm, cmd_str)
        _PROCESSES[provider_norm] = proc
        _META[provider_norm] = {"cmd": cmd_str, "started_at": time.time()}
        return {"status": "started", "provider": provider_norm, "pid": proc.pid, "cmd": cmd_str}


def stop_provider(provider: str, timeout: float = 10.0) -> dict:
    provider_norm = normalize_provider(provider)
    with _LOCK:
        # Always consult canonical registry first and stop by PID
        entry = _get_registry_entry_canonical(provider_norm)
        if entry and isinstance(entry.get("pid"), int):
            pid = int(entry["pid"])
            result = _stop_by_pid(pid, timeout=timeout)
            status = result.get("status")
            # Clean up tracking and registry for successful or already-not-running states
            if status in {"stopped_by_pid", "killed_by_pid", "not_running_pid"}:
                _remove_from_registry(provider_norm)
            # Drop any in-memory references regardless of outcome
            _PROCESSES.pop(provider_norm, None)
            meta = _META.pop(provider_norm, {})
            result.update({"provider": provider_norm, "meta": meta})
            return result

        # If the provider is not in the registry, report not_found
        return {"status": "not_found", "provider": provider_norm}


def active_providers() -> list[str]:
    """Return a list of provider keys that are currently running."""
    with _LOCK:
        return [name for name, proc in _PROCESSES.items() if proc and proc.poll() is None]


def stop_all(timeout: float = 10.0) -> dict:
    """Stop all running provider processes.

    Returns a summary dict with per-provider statuses.
    """
    results: dict[str, dict] = {}
    # First stop those we actively track
    for name in list(active_providers()):
        try:
            results[name] = stop_provider(name, timeout=timeout)
        except Exception as e:
            results[name] = {"status": "error", "error": str(e)}
    # Then stop any orphaned PIDs from the registry
    try:
        reg = _load_registry()
        for name, entry in reg.items():
            if name in results:
                continue
            pid = entry.get("pid")
            if isinstance(pid, int):
                res = _stop_by_pid(pid, timeout=timeout)
                results[name] = res
                if res.get("status") in {"stopped_by_pid", "killed_by_pid"}:
                    _remove_from_registry(name)
    except Exception as e:
        results["_registry_error"] = {"status": "error", "error": str(e)}
    return {"stopped": results}


# Restart persistence and auto-restore functionality removed per request.