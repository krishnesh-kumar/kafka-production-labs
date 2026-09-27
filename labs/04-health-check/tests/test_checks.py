from healthcheck.checks import run_all
from healthcheck.model import Group, Partition, Snapshot, Topic

GOOD_BROKER = {
    "unclean.leader.election.enable": "false",
    "auto.create.topics.enable": "false",
    "default.replication.factor": "3",
    "offsets.topic.replication.factor": "3",
    "min.insync.replicas": "2",
}


def topic(name, rf=3, min_isr="2", partitions=3, isr_missing=0, leader_offline=False, **cfg):
    brokers = [1, 2, 3][:rf]
    parts = []
    for i in range(partitions):
        leader = -1 if leader_offline else brokers[i % len(brokers)]
        isr = brokers[: len(brokers) - isr_missing] if i == 0 else list(brokers)
        parts.append(Partition(i, leader, list(brokers), isr))
    return Topic(name, parts, {"min.insync.replicas": min_isr, "cleanup.policy": "delete",
                               "retention.ms": "604800000", **cfg})


def snap(topics=(), groups=(), brokers=(1, 2, 3), broker_config=None):
    return Snapshot("test:9092", "abc", list(brokers), 1, dict(broker_config or GOOD_BROKER), list(topics), list(groups))


def titles(findings):
    return [f.title for f in findings]


def test_healthy_cluster_has_no_findings():
    assert run_all(snap([topic("payments", partitions=6)])) == []


def test_min_isr_equal_to_rf_is_critical():
    f = run_all(snap([topic("ledger", min_isr="3")]))
    assert f[0].severity == "CRITICAL"
    assert "min.insync.replicas equals" in f[0].title


def test_rf1_on_three_brokers_is_critical():
    f = run_all(snap([topic("orders", rf=1, min_isr="1")]))
    assert any(x.severity == "CRITICAL" and "replication factor 1" in x.title for x in f)


def test_under_replicated_and_offline():
    f = run_all(snap([topic("a", isr_missing=1), topic("b", leader_offline=True)]))
    t = titles(f)
    assert any("under-replicated" in x for x in t)
    assert any("offline" in x for x in t)
    assert f[0].severity == "CRITICAL"


def test_weak_durability_rf3_min_isr1():
    f = run_all(snap([topic("events", min_isr="1")]))
    assert titles(f) == ["RF=3 but min.insync.replicas=1"]


def test_unbounded_retention_only_for_delete_policy():
    f = run_all(snap([topic("audit", **{"retention.ms": "-1"}),
                      topic("customers-cdc", **{"retention.ms": "-1", "cleanup.policy": "compact"})]))
    forever = [x for x in f if "Unbounded" in x.title]
    assert len(forever) == 1 and forever[0].subjects == ["audit"]


def test_cluster_level_settings():
    bad = dict(GOOD_BROKER, **{"unclean.leader.election.enable": "true", "auto.create.topics.enable": "true",
                               "offsets.topic.replication.factor": "1"})
    t = titles(run_all(snap(broker_config=bad)))
    assert "Unclean leader election is enabled" in t
    assert "Topics are auto-created" in t
    assert "__consumer_offsets RF=1" in t


def test_small_cluster():
    f = run_all(snap(brokers=[1], topics=[topic("x", rf=1, min_isr="1")]))
    assert any("Only 1 broker" in x.title for x in f)


def test_consumer_groups():
    groups = [Group("invoice-service", "Empty", 0, {"orders": 4000}),
              Group("search-indexer", "Stable", 2, {"orders": 25_000}),
              Group("healthy", "Stable", 3, {"orders": 10})]
    t = titles(run_all(snap([topic("orders")], groups)))
    assert "1 consumer group(s) with lag and no active members" in t
    assert "1 consumer group(s) above 10,000 lag" in t


def test_findings_sorted_by_severity():
    f = run_all(snap([topic("audit", **{"retention.ms": "-1"}), topic("ledger", min_isr="3")]))
    assert [x.severity for x in f] == sorted([x.severity for x in f],
                                             key=["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"].index)
