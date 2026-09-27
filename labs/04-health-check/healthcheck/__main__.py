import argparse
import sys

from .checks import run_all
from .collect import collect
from .report import render


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m healthcheck",
                                     description="Read-only first-pass health check for a Kafka cluster.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="bootstrap servers")
    parser.add_argument("--out", help="write the markdown report here (default: stdout)")
    parser.add_argument("--fail-on", choices=["CRITICAL", "HIGH", "MEDIUM", "LOW", "never"], default="never",
                        help="exit 1 if a finding at this severity or worse exists (for CI)")
    parser.add_argument("--config", action="append", default=[], metavar="KEY=VALUE",
                        help="extra client config, e.g. security.protocol=SASL_SSL (repeatable)")
    args = parser.parse_args()

    extra = dict(item.split("=", 1) for item in args.config)
    snapshot = collect(args.bootstrap, extra)
    findings = run_all(snapshot)
    report = render(snapshot, findings)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(report)
        print(f"Report written to {args.out}: {len(findings)} finding(s)")
    else:
        print(report)

    if args.fail_on != "never":
        order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        worst_allowed = order.index(args.fail_on)
        if any(order.index(f.severity) <= worst_allowed for f in findings if f.severity in order):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
