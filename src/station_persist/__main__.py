from __future__ import annotations

import argparse
import asyncio
import json

from station_persist.config import Settings
from station_persist.consumption import run_consumption
from station_persist.logging_config import configure_logging
from station_persist.operations import run_operation
from station_persist.service import run_inspection


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Telemetry inspection and opt-in persistence"
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=["inspect", "consume", "status", "quarantine"],
        default="inspect",
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
    parser.add_argument("action", nargs="?", choices=["list", "show", "replay"])
    parser.add_argument("--id", type=int, dest="quarantine_id")
    parser.add_argument("--request-id")
    parser.add_argument("--operator")
    parser.add_argument("--reason")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--after-id", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "quarantine":
        if not args.action or (
            args.action in ("show", "replay") and not args.quarantine_id
        ):
            parser.error("quarantine action and --id are required")
        if args.action == "replay" and not all(
            (args.request_id, args.operator, args.reason)
        ):
            parser.error("replay requires --request-id, --operator and --reason")
    elif args.action is not None:
        parser.error("action belongs to quarantine")
    if (
        not 1 <= args.limit <= 1000
        or args.after_id < 0
        or (args.quarantine_id is not None and args.quarantine_id < 1)
    ):
        parser.error("invalid pagination or quarantine ID")
    if args.dry_run and (args.command != "quarantine" or args.action != "replay"):
        parser.error("--dry-run belongs to quarantine replay")
    if args.command != "inspect" and (args.resolve or args.validate):
        parser.error(
            "--resolve and --validate belong to inspect; consume always validates"
        )
    settings = Settings()
    configure_logging(settings.log_level)
    try:
        if args.command in ("status", "quarantine"):
            result = asyncio.run(
                run_operation(
                    settings,
                    args.command,
                    action=args.action,
                    quarantine_id=args.quarantine_id,
                    request_id=args.request_id,
                    operator=args.operator,
                    reason=args.reason,
                    limit=args.limit,
                    after_id=args.after_id,
                    dry_run=args.dry_run,
                )
            )
            print(json.dumps(result, ensure_ascii=True, default=str))
            if result.get("degraded"):
                raise SystemExit(1)
            return
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
