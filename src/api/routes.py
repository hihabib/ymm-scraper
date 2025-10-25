from fastapi import APIRouter, HTTPException
import psutil
import shutil
from pathlib import Path
from .utils.response import success, error_json

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/status")
def system_status():
    """Return current system metrics: RAM, storage, CPU usage."""
    try:
        # RAM
        vm = psutil.virtual_memory()
        total_ram_gb = round(vm.total / (1024 ** 3), 2)
        used_ram_gb = round((vm.total - vm.available) / (1024 ** 3), 2)
        ram_usage_percent = vm.percent

        # Storage (root partition / drive anchor)
        root_path = Path(__file__).resolve().anchor or "/"
        du = shutil.disk_usage(root_path)
        total_disk_gb = round(du.total / (1024 ** 3), 2)
        used_disk_gb = round(du.used / (1024 ** 3), 2)
        disk_usage_percent = round((du.used / du.total) * 100, 2) if du.total else 0.0

        # CPU percent over a short interval
        cpu_percent = psutil.cpu_percent(interval=0.2)

        data = {
            "ram": {
                "totalGB": total_ram_gb,
                "usedGB": used_ram_gb,
                "usagePercent": ram_usage_percent,
            },
            "storage": {
                "totalGB": total_disk_gb,
                "usedGB": used_disk_gb,
                "usagePercent": disk_usage_percent,
                "path": root_path,
            },
            "processor": {
                "usagePercent": cpu_percent,
            },
        }

        return success(data=data, message="System status fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch system status: {e}", status_code=500)