from collections import Counter
from datetime import datetime, timezone

from .model import SEVERITY_ORDER, Snapshot

ICON = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵", "INFO": "⚪"}


def render(snapshot: Snapshot, findings) -> str:
    counts = Counter(f.severity for f in findings)
    user_topics = [t for t in snapshot.topics if not t.name.startswith("_")]
    lines = [
        "# Kafka health check (automated first pass)",
        "",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} by kafka-healthcheck-lite. Read-only: nothing was changed.",
        "",
        "## Cluster at a glance",
        "",
        "| | |",
        "|---|---|",
        f"| Cluster id | `{snapshot.cluster_id}` |",
        f"| Brokers | {len(snapshot.brokers)} (ids {', '.join(map(str, snapshot.brokers))}; controller {snapshot.controller}) |",
        f"| Topics | {len(user_topics)} user topics, {sum(len(t.partitions) for t in user_topics)} partitions |",
        f"| Consumer groups | {len(snapshot.groups)} |",
        "",
        "## Summary",
        "",
        "| Severity | Findings |",
        "|---|---|",
    ]
    for sev in SEVERITY_ORDER:
        if counts.get(sev):
            lines.append(f"| {ICON[sev]} {sev} | {counts[sev]} |")
    if not findings:
        lines.append("| ✅ none | 0 |")

    if findings:
        lines += ["", "## Fix first", ""]
        for i, f in enumerate(findings[:3], 1):
            lines.append(f"{i}. **{f.title}** ({f.severity.lower()}): {f.fix}")

    lines += ["", "## Findings", ""]
    for f in findings:
        lines += [f"### {ICON[f.severity]} {f.severity} · {f.area} · {f.title}", "", f.detail, "", f"**Fix:** {f.fix}", ""]
        if f.subjects:
            shown = f.subjects[:15]
            lines += [f"- `{s}`" for s in shown]
            if len(f.subjects) > len(shown):
                lines.append(f"- … and {len(f.subjects) - len(shown)} more")
            lines.append("")

    lines += [
        "## Not covered by this automated pass",
        "",
        "A full review also looks at: producer settings in each service (acks, idempotence, retries, batching), "
        "consumer commit strategy and rebalance behaviour, Kafka Connect / Debezium (replication-slot lag, "
        "snapshot and schema-change handling), security (TLS, SASL, ACLs), broker JVM/disk/network metrics, "
        "and whether the alerting would actually page someone.",
        "",
    ]
    return "\n".join(lines)
