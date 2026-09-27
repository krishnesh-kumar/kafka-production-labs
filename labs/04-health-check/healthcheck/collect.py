"""Read-only collection. Uses only describe/list APIs: nothing on the cluster is changed."""
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, OffsetSpec, ResourceType

from .model import Group, Partition, Snapshot, Topic

TIMEOUT = 15


def _configs(admin, resources):
    out = {}
    if not resources:
        return out
    for resource, future in admin.describe_configs(resources, request_timeout=TIMEOUT).items():
        out[resource.name] = {name: entry.value for name, entry in future.result().items()}
    return out


def collect(bootstrap: str, extra: dict | None = None) -> Snapshot:
    admin = AdminClient({"bootstrap.servers": bootstrap, **(extra or {})})

    metadata = admin.list_topics(timeout=TIMEOUT)
    cluster = admin.describe_cluster(request_timeout=TIMEOUT).result()
    brokers = sorted(n.id for n in cluster.nodes)
    controller = cluster.controller.id if cluster.controller else -1

    broker_config = _configs(admin, [ConfigResource(ResourceType.BROKER, str(brokers[0]))]).get(str(brokers[0]), {})
    topic_names = sorted(metadata.topics)
    topic_config = _configs(admin, [ConfigResource(ResourceType.TOPIC, t) for t in topic_names])

    topics = []
    for name in topic_names:
        tm = metadata.topics[name]
        parts = [Partition(p.id, p.leader, list(p.replicas), list(p.isrs)) for p in tm.partitions.values()]
        topics.append(Topic(name, sorted(parts, key=lambda p: p.id), topic_config.get(name, {})))

    groups = []
    listing = admin.list_consumer_groups(request_timeout=TIMEOUT).result()
    group_ids = sorted(g.group_id for g in listing.valid)
    if group_ids:
        described = {gid: f.result() for gid, f in admin.describe_consumer_groups(group_ids, request_timeout=TIMEOUT).items()}
        committed = {}
        for gid in group_ids:
            futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(gid)], request_timeout=TIMEOUT)
            for _, f in futures.items():
                committed[gid] = [tp for tp in f.result().topic_partitions if tp.offset >= 0]
        wanted = {TopicPartition(tp.topic, tp.partition): OffsetSpec.latest()
                  for tps in committed.values() for tp in tps}
        ends = {}
        if wanted:
            for tp, f in admin.list_offsets(wanted, request_timeout=TIMEOUT).items():
                ends[(tp.topic, tp.partition)] = f.result().offset
        for gid in group_ids:
            lag = {}
            for tp in committed.get(gid, []):
                end = ends.get((tp.topic, tp.partition))
                if end is not None:
                    lag[tp.topic] = lag.get(tp.topic, 0) + max(0, end - tp.offset)
            d = described[gid]
            groups.append(Group(gid, str(d.state).split(".")[-1], len(d.members), lag))

    return Snapshot(bootstrap, cluster.cluster_id, brokers, controller, broker_config, topics, groups)
