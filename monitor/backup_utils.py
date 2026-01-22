from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

# Py 3.11+: tomllib is stdlib; fallback to tomli for 3.10/3.9
try:
    import tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


def get_backup_location() -> Optional[Path]:
    """
    Read the backup location from settings.toml.
    Returns None if backup is disabled or settings file doesn't exist.
    """
    settings_path = Path(__file__).parent / "settings.toml"
    
    if not settings_path.exists():
        return None
    
    try:
        with settings_path.open("rb") as f:
            settings = tomllib.load(f)
        
        backup_loc = settings.get("general", {}).get("backup_location", "")
        if backup_loc and backup_loc.strip():
            return Path(backup_loc.strip())
    except Exception:
        pass
    
    return None


def save_backup_snapshot(primary_snapshot_path: Path, award_id: str) -> Optional[Path]:
    """
    Save a copy of the snapshot to the backup location.
    
    Args:
        primary_snapshot_path: Path to the snapshot in the primary location
        award_id: The award ID (used for organizing backups)
    
    Returns:
        Path to the backup file if successful, None otherwise
    """
    backup_root = get_backup_location()
    
    if backup_root is None:
        print(f"[{award_id}] Backup disabled: No backup location configured")
        return None
    
    if not primary_snapshot_path.exists():
        print(f"[{award_id}] Backup skipped: Primary snapshot not found at {primary_snapshot_path}")
        return None
    
    try:
        # Create backup directory structure: backups/{award_id}/
        backup_dir = backup_root / award_id
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Use the same filename as the primary snapshot
        backup_path = backup_dir / primary_snapshot_path.name
        
        # Copy the file
        shutil.copy2(primary_snapshot_path, backup_path)
        
        print(f"[{award_id}] SUCCESS: Backup saved to: {backup_path}")
        return backup_path
    
    except PermissionError as e:
        print(f"[{award_id}] Warning: Permission denied saving backup to {backup_root} - {e}")
        print(f"[{award_id}] Check that you have write access to the backup location")
        return None
    except FileNotFoundError as e:
        print(f"[{award_id}] Warning: Backup location not found: {backup_root} - {e}")
        print(f"[{award_id}] Check that the network drive is connected and accessible")
        return None
    except OSError as e:
        # Handle network-related errors (like drive disconnected)
        if hasattr(e, 'winerror') and e.winerror in (53, 67, 1200):  # Network path not found, network name not found, etc.
            print(f"[{award_id}] Warning: Network drive error saving backup to {backup_root} - {e}")
            print(f"[{award_id}] Check that the network drive is connected and accessible")
        else:
            print(f"[{award_id}] Warning: OS error saving backup to {backup_root} - {e}")
        return None
    except Exception as e:
        print(f"[{award_id}] Warning: Failed to save backup - {e}")
        return None
