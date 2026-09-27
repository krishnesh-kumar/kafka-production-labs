"""Pure checks over a Snapshot. No network access here, so every rule is unit-tested."""
from .model import Finding, Snapshot

LAG_THRESHOLD = 10_000


def _int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _true(value) -> bool:
    return str(value).lower() == "true"


def cluster_checks(s: Snapshot):
    cfg = s.broker_config
    if len(s.brokers) < 3:
        yield Finding("HIGH", "Cluster", f"Only {len(s.brokers)} broker(s)",
                      "With fewer than 3 brokers you cannot run RF=3 with min.insync.replicas=2, so one broker "
                      "restart either stops acks=all writes or risks losing acknowledged data.",
                      "Run at least 3 brokers across failure domains (zones/nodes).")
    if _true(cfg.get("unclean.leader.election.enable")):
        yield Finding("HIGH", "Cluster", "Unclean leader election is enabled",
                      "An out-of-sync replica can become leader and silently drop acknowledged records.",
                      "Set unclean.leader.election.enable=false; handle the rare offline partition by hand.")
    if _true(cfg.get("auto.create.topics.enable")):
        yield Finding("MEDIUM", "Cluster", "Topics are auto-created",
                      "A typo in a producer creates a new topic with default settings, often RF=1.",
                      "Set auto.create.topics.enable=false and create topics through code review / IaC.")
    rf = _int(cfg.get("default.replication.factor"))
    if rf is not None and rf < 3 and len(s.brokers) >= 3:
        yield Finding("MEDIUM", "Cluster", f"default.replication.factor={rf}",
                      "Any topic created without an explicit RF inherits it.",
                      "Set default.replication.factor=3.")
    offsets_rf = _int(cfg.get("offsets.topic.replication.factor"))
    if offsets_rf is not None and offsets_rf < 3 and len(s.brokers) >= 3:
        yield Finding("HIGH", "Cluster", f"__consumer_offsets RF={offsets_rf}",
                      "Losing the broker that holds committed offsets makes consumer groups stall or rewind.",
                      "Recreate the cluster with offsets.topic.replication.factor=3, or reassign "
                      "__consumer_offsets partitions to 3 replicas.")
    min_isr = _int(cfg.get("min.insync.replicas"))
    if min_isr is not None and min_isr < 2 and len(s.brokers) >= 3:
        yield Finding("MEDIUM", "Cluster", f"Broker default min.insync.replicas={min_isr}",
                      "acks=all only waits for the leader, so one disk failure can lose acknowledged writes.",
                      "Set min.insync.replicas=2 as the broker default (with RF=3).")


def topic_checks(s: Snapshot):
    user_topics = [t for t in s.topics if not t.name.startswith("_")]
    by_rf = {}
    strict, weak, unclean, forever, urp, offline = [], [], [], [], [], []
    for t in user_topics:
        rf = t.replication_factor
        if rf < 3:
            by_rf.setdefault(rf, []).append(t.name)
        min_isr = _int(t.cfg("min.insync.replicas"), _int(s.broker_config.get("min.insync.replicas"), 1))
        if rf > 1 and min_isr >= rf:
            strict.append(f"{t.name} (RF={rf}, min.insync.replicas={min_isr})")
        elif rf >= 3 and min_isr < 2:
            weak.append(t.name)
        if _true(t.cfg("unclean.leader.election.enable")):
            unclean.append(t.name)
        policy = str(t.cfg("cleanup.policy", "delete"))
        if "compact" not in policy and str(t.cfg("retention.ms")) == "-1" and str(t.cfg("retention.bytes", "-1")) == "-1":
            forever.append(t.name)
        for p in t.partitions:
            if p.leader < 0:
                offline.append(f"{t.name}-{p.id}")
            elif len(p.isr) < len(p.replicas):
                urp.append(f"{t.name}-{p.id}")

    if offline:
        yield Finding("CRITICAL", "Topics", f"{len(offline)} offline partition(s)",
                      "These partitions have no leader: reads and writes fail right now.",
                      "Bring the replica brokers back; do not enable unclean election without accepting data loss.",
                      offline)
    if strict:
        yield Finding("CRITICAL", "Topics", "min.insync.replicas equals the replication factor",
                      "Any single broker restart (a rolling upgrade, a node drain) rejects every acks=all write "
                      "to these topics with NotEnoughReplicas.",
                      "Use RF=3 with min.insync.replicas=2.", strict)
    for rf, names in sorted(by_rf.items()):
        sev = "CRITICAL" if rf == 1 and len(s.brokers) >= 3 else "HIGH"
        yield Finding(sev, "Topics", f"{len(names)} topic(s) with replication factor {rf}",
                      "One broker or disk failure loses these partitions" + (" entirely." if rf == 1 else
                      " or blocks writes."),
                      "Increase to RF=3 with kafka-reassign-partitions (plan it: it copies all data).", names)
    if urp:
        yield Finding("HIGH", "Topics", f"{len(urp)} under-replicated partition(s)",
                      "Followers are behind or a broker is down. One more failure and these partitions stop "
                      "accepting acks=all writes.",
                      "Find the lagging broker (disk, network, GC) before it becomes an outage.", urp)
    if unclean:
        yield Finding("HIGH", "Topics", "Unclean leader election enabled per topic",
                      "Topic-level override allows silent data loss on failover.",
                      "Remove the override.", unclean)
    if weak:
        yield Finding("MEDIUM", "Topics", "RF=3 but min.insync.replicas=1",
                      "acks=all returns after the leader alone has the write; replication buys availability, "
                      "not durability.", "Set min.insync.replicas=2 on these topics.", weak)
    if forever:
        yield Finding("LOW", "Topics", "Unbounded retention on non-compacted topics",
                      "retention.ms=-1 with delete policy grows forever; disks fill slowly, then suddenly.",
                      "Set a retention that matches replay needs, or tier to object storage.", forever)

    # Leader balance across brokers
    leaders = {b: 0 for b in s.brokers}
    for t in user_topics:
        for p in t.partitions:
            if p.leader in leaders:
                leaders[p.leader] += 1
    if len(leaders) >= 2 and sum(leaders.values()) >= 6:
        lo, hi = min(leaders.values()), max(leaders.values())
        if lo == 0 or hi / lo > 1.5:
            yield Finding("LOW", "Topics", "Partition leadership is unbalanced",
                          f"Leaders per broker: {leaders}. The busiest broker does most of the work.",
                          "Run a preferred leader election (kafka-leader-election.sh --election-type PREFERRED) "
                          "and check auto.leader.rebalance.enable.")


def group_checks(s: Snapshot):
    lagging, stuck = [], []
    for g in s.groups:
        if g.total_lag <= 0:
            continue
        label = f"{g.group_id}: lag {g.total_lag:,} ({', '.join(f'{t}={n:,}' for t, n in sorted(g.lag_by_topic.items()))})"
        if g.members == 0:
            stuck.append(label)
        elif g.total_lag > LAG_THRESHOLD:
            lagging.append(label)
    if stuck:
        yield Finding("MEDIUM", "Consumers", f"{len(stuck)} consumer group(s) with lag and no active members",
                      "Nobody is consuming these offsets. Either a service is down or the group is abandoned "
                      "and should be deleted so it stops confusing lag alerts.",
                      "Confirm the owning service; delete dead groups with kafka-consumer-groups.sh --delete.", stuck)
    if lagging:
        yield Finding("MEDIUM", "Consumers", f"{len(lagging)} consumer group(s) above {LAG_THRESHOLD:,} lag",
                      "Consumers are behind producers. If lag keeps growing, data freshness SLOs are at risk.",
                      "Check processing time per record, partition count vs consumer count, and rebalances.",
                      lagging)


def run_all(s: Snapshot):
    findings = [*cluster_checks(s), *topic_checks(s), *group_checks(s)]
    return sorted(findings, key=lambda f: f.rank())
