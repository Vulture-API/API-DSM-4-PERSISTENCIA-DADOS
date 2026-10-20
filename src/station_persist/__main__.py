from __future__ import annotations

import argparse
import asyncio

from station_persist.config import Settings
from station_persist.logging_config import configure_logging
from station_persist.service import run_inspection


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read-only telemetry inspection")
    parser.add_argument("command", nargs="?", choices=["inspect"], default="inspect")
    parser.add_argument(
        "--resolve",
        action="store_true",
        help="Diagnose catalog associations (read-only)",
    )
    args = parser.parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level)
    try:
        asyncio.run(
            run_inspection(settings, resolve=True)
            if args.resolve
            else run_inspection(settings)
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
