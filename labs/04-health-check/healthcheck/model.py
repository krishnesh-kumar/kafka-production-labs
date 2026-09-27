from dataclasses import dataclass, field


@dataclass
class Partition:
    id: int
    leader: int  # -1 when offline
    replicas: list
    isr: list


@dataclass
class Topic:
    name: str
    partitions: list
    config: dict = field(default_factory=dict)

    @property
    def replication_factor(self) -> int:
        return min((len(p.replicas) for p in self.partitions), default=0)

    def cfg(self, key, default=None):
        return self.config.get(key, default)


@dataclass
class Group:
    group_id: str
    state: str
    members: int
    lag_by_topic: dict = field(default_factory=dict)  # topic -> total lag

    @property
    def total_lag(self) -> int:
        return sum(self.lag_by_topic.values())


@dataclass
class Snapshot:
    bootstrap: str
    cluster_id: str
    brokers: list  # broker ids
    controller: int
    broker_config: dict  # config of one broker, name -> value
    topics: list
    groups: list


SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]


@dataclass
class Finding:
    severity: str
    area: str
    title: str
    detail: str
    fix: str
    subjects: list = field(default_factory=list)

    def rank(self) -> int:
        return SEVERITY_ORDER.index(self.severity)
