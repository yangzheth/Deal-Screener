from __future__ import annotations

import os
import shutil
import subprocess
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; AIPrimaryMarketWatch/0.2)"


def fetch_url_bytes(url: str, *, user_agent: str = USER_AGENT, timeout: int = 20) -> bytes:
    request = Request(url, headers={"User-Agent": user_agent})
    try:
        with urlopen(request, timeout=timeout) as response:
            return response.read()
    except Exception as exc:
        if os.name != "nt":
            raise
        curl_path = shutil.which("curl.exe") or shutil.which("curl")
        if not curl_path:
            raise
        completed = subprocess.run(
            [curl_path, "-sSL", "-A", user_agent, "--max-time", str(timeout), url],
            capture_output=True,
        )
        if completed.returncode != 0:
            raise exc
        return completed.stdout
