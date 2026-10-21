from __future__ import annotations

import argparse
import asyncio

from station_persist.config import Settings
from station_persist.consumption import run_consumption
from station_persist.logging_config import configure_logging
from station_persist.service import run_inspection


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Telemetry inspection and opt-in persistence"
    )
    parser.add_argument(
        "command", nargs="?", choices=["inspect", "consume"], default="inspect"
    )
    parser.add_argument(
        "--resolve",
        action="store_true",
        help="Diagnose catalog associations (read-only)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate telemetry (read-only, includes resolution)",
    )
    args = parser.parse_args(argv)
    if args.command == "consume" and (args.resolve or args.validate):
        parser.error(
            "--resolve and --validate belong to inspect; consume always validates"
        )
    settings = Settings()
    configure_logging(settings.log_level)
    try:
        asyncio.run(
            run_consumption(settings)
            if args.command == "consume"
            else run_inspection(settings, validate=True)
            if args.validate
            else run_inspection(settings, resolve=True)
            if args.resolve
            else run_inspection(settings)
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
