from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class QueryRecord:
    """
    Represents a single captured query with its fingerprints.
    This is what gets passed in from the capture job after
    fingerprint.py has processed it.
    """
    query_id:        str
    template_hash:   str
    sweep_hash:      str
    refine_hash:     str
    query_text:      str
    topology:        dict
    signature:       dict
    warehouse_name:  Optional[str] = None
    execution_time_ms: Optional[int] = None
    bytes_scanned:   Optional[int] = None
    credits_used:    Optional[float] = None


@dataclass
class RefineCluster:
    """
    A sub-cluster of queries sharing both sweep and refine fingerprints.
    These are the materialization candidates.
    """
    sweep_hash:        str
    refine_hash:       str
    topology:          dict
    signature:         dict
    query_count:       int = 0
    template_count:    int = 0
    total_credits:     float = 0.0
    total_exec_ms:     int = 0
    query_ids:         list = field(default_factory=list)
    template_hashes:   list = field(default_factory=list)
    flagged:           bool = False


@dataclass
class SweepCluster:
    """
    A broad cluster of queries sharing the same join topology.
    Contains one or more refine sub-clusters.
    """
    sweep_hash:      str
    topology:        dict
    query_count:     int = 0
    template_count:  int = 0
    total_credits:   float = 0.0
    refine_clusters: list = field(default_factory=list)
    flagged:         bool = False


def run_sweep(queries: list[QueryRecord]) -> dict[str, SweepCluster]:
    """
    Broad pass. Groups queries by sweep_hash.
    Returns a dict of sweep_hash -> SweepCluster.
    """
    clusters: dict[str, SweepCluster] = {}

    for q in queries:
        if q.sweep_hash not in clusters:
            clusters[q.sweep_hash] = SweepCluster(
                sweep_hash=q.sweep_hash,
                topology=q.topology,
            )

        cluster = clusters[q.sweep_hash]
        cluster.query_count += 1
        cluster.total_credits += q.credits_used or 0.0

    return clusters


def run_refine(queries: list[QueryRecord]) -> dict[str, dict[str, RefineCluster]]:
    """
    Refine pass. Groups queries by sweep_hash then refine_hash.
    Returns a nested dict: sweep_hash -> refine_hash -> RefineCluster.
    """
    clusters: dict[str, dict[str, RefineCluster]] = defaultdict(dict)

    for q in queries:
        sweep = q.sweep_hash
        refine = q.refine_hash

        if refine not in clusters[sweep]:
            clusters[sweep][refine] = RefineCluster(
                sweep_hash=sweep,
                refine_hash=refine,
                topology=q.topology,
                signature=q.signature,
            )

        cluster = clusters[sweep][refine]
        cluster.query_count += 1
        cluster.total_credits += q.credits_used or 0.0
        cluster.total_exec_ms += q.execution_time_ms or 0

        cluster.query_ids.append(q.query_id)

        # track distinct templates -- one query run 50 times still
        # counts as one template, not 50 independent queries
        if q.template_hash not in cluster.template_hashes:
            cluster.template_hashes.append(q.template_hash)

        cluster.template_count = len(cluster.template_hashes)

    return clusters


def flag_clusters(
    refine_clusters: dict[str, dict[str, RefineCluster]],
    min_templates: int = 3,
) -> list[RefineCluster]:
    """
    Flags refine sub-clusters that meet the threshold for operator review.
    Default threshold is 3 distinct query templates sharing the same
    sweep and refine fingerprint.

    Returns a flat list of flagged clusters sorted by total_credits desc
    so the operator sees the highest-cost candidates first.
    """
    flagged = []

    for sweep_hash, refine_map in refine_clusters.items():
        for refine_hash, cluster in refine_map.items():
            if cluster.template_count >= min_templates:
                cluster.flagged = True
                flagged.append(cluster)

    return sorted(flagged, key=lambda c: c.total_credits, reverse=True)


def run_clustering(
    queries: list[QueryRecord],
    min_templates: int = 3,
) -> tuple[dict[str, SweepCluster], list[RefineCluster]]:
    """
    Full clustering pipeline. Runs sweep, refine, and flagging
    in a single call. Returns the sweep clusters and the flagged
    refine sub-clusters.

    This is the main entry point for the capture job to call
    after fingerprinting a batch of queries.
    """
    sweep_clusters = run_sweep(queries)
    refine_clusters = run_refine(queries)
    flagged = flag_clusters(refine_clusters, min_templates=min_templates)

    # roll up template counts to sweep clusters
    for sweep_hash, refine_map in refine_clusters.items():
        all_templates = set()
        for cluster in refine_map.values():
            all_templates.update(cluster.template_hashes)
        if sweep_hash in sweep_clusters:
            sweep_clusters[sweep_hash].template_count = len(all_templates)
            sweep_clusters[sweep_hash].refine_clusters = list(refine_map.values())
            sweep_clusters[sweep_hash].flagged = any(
                c.flagged for c in refine_map.values()
            )

    return sweep_clusters, flagged
