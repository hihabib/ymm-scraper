def make_full_key(year: str, make: str, model: str, trim: str, drive: str, vehicle_type: str, boltpattern: str, drchassisid: str) -> str:
    return f"{year}__{make}__{model}__{trim}__{drive}__{vehicle_type}__{boltpattern}__{drchassisid}"


def make_base_key(year: str, make: str, model: str, trim: str, drive: str) -> str:
    """Create a base key with just the 5 basic parameters for filtering purposes."""
    return f"{year}__{make}__{model}__{trim}__{drive}"


def make_full_pref_key(
    year: str,
    make: str,
    model: str,
    trim: str,
    drive: str,
    vehicle_type: str,
    boltpattern: str,
    drchassisid: str,
    suspension: str,
    modification: str,
    rubbing: str,
) -> str:
    return (
        f"{year}__{make}__{model}__{trim}__{drive}__{vehicle_type}__{boltpattern}__{drchassisid}__{suspension}__{modification}__{rubbing}"
    )