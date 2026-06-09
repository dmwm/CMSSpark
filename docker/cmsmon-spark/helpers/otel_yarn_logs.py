#!/usr/bin/env python
"""
Pipe spark-submit stdout/stderr through this script to forward lines to OTEL.

Usage in cron scripts (when OTEL_ENABLED=true):

    util_spark_submit_with_otel_logs "${spark_submit_args[@]}" script.py args...

Or manually:

    spark-submit ... 2>&1 | python3 /data/helpers/otel_yarn_logs.py
"""

from __future__ import annotations

import sys

from helpers.otel_setup import (
    log_yarn_line,
    otel_config,
    should_export_yarn_line,
    shutdown_opentelemetry,
)


def main() -> int:
    try:
        for line in sys.stdin:
            if should_export_yarn_line(line):
                sys.stdout.write(line)
                sys.stdout.flush()
                log_yarn_line(line)
    finally:
        if otel_config()["enabled"]:
            shutdown_opentelemetry()
    return 0


if __name__ == "__main__":
    sys.exit(main())
