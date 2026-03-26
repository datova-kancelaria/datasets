from __future__ import annotations

import os
from pathlib import Path

import requests

from .models import AppSettings, Credentials


def read_cred(creds_dir: Path, name: str) -> str:
    p = creds_dir / name
    b = p.read_bytes()
    while b.endswith(b"\n") or b.endswith(b"\r"):
        b = b[:-1]
    return b.decode("utf-8")


def load_credentials() -> Credentials:
    creds_dir_env = os.environ.get("CREDENTIALS_DIRECTORY")
    if not creds_dir_env:
        raise RuntimeError(
            "CREDENTIALS_DIRECTORY not set "
            "(run under systemd with LoadCredential=... or systemd-run -p LoadCredential=...)"
        )
    creds_dir = Path(creds_dir_env)
    return Credentials(
        apikey=read_cred(creds_dir, "APIKEY"),
        user=read_cred(creds_dir, "USER"),
        password=read_cred(creds_dir, "PASS"),
    )


def build_session(creds: Credentials) -> requests.Session:
    s = requests.Session()
    s.auth = (creds.user, creds.password)
    s.trust_env = (os.environ.get("CES_TRUST_ENV") == "1")
    return s


def common_headers(creds: Credentials) -> dict[str, str]:
    return {"APIKey": creds.apikey, "Accept": "application/json"}


def default_settings() -> AppSettings:
    return AppSettings()
