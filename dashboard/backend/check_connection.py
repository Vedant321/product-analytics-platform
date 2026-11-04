"""Quick local credential check before starting a live dashboard."""

from __future__ import annotations

import os
import sys

from databricks.sdk import WorkspaceClient


def main() -> int:
    if "your-workspace" in os.getenv("DATABRICKS_HOST", ""):
        print("DATABRICKS_HOST is a placeholder. Unset it or configure a real workspace profile.", file=sys.stderr)
        return 1
    try:
        client = WorkspaceClient()
        client.current_user.me()
    except Exception as exc:
        if type(exc).__name__ == "PermissionDenied":
            print("Databricks rejected the local credentials. Sign in with OAuth or renew the token.", file=sys.stderr)
        else:
            print(f"Databricks connection check failed: {type(exc).__name__}.", file=sys.stderr)
        return 1
    print("Databricks authentication is valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
