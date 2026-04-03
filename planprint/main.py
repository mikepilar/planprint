# -----------------------------------------------------------------------------
# planprint
# main.py
#
# Author:      Mike Pilar
# Created:     2026-04-03
# Description: Primary pipeline entry point. Orchestrates the full planprint
#              workflow -- capture, fingerprint, cluster, and flag. Can be run
#              directly as a script or imported and called from a scheduler.
# -----------------------------------------------------------------------------

import logging
from planprint.capture import run_capture
from planprint.cluster import run_clustering

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

logger = logging.getLogger(__name__)


def run_pipeline(
    user: str,
    password: str,
    account: str,
    warehouse: str,
    role: str = None,
    lookback_hours: int = 23,
    min_execution_ms: int = 1000,
    limit: int = 500,
    min_templates: int = 3,
) -> dict:
    """
    Full planprint pipeline. Captures, fingerprints, clusters,
    and flags in a single call. Returns a summary dict suitable
    for logging or downstream reporting.
    """

    # step 1 -- capture and fingerprint
    logger.info("Starting capture")
    records = run_capture(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        role=role,
        lookback_hours=lookback_hours,
        min_execution_ms=min_execution_ms,
        limit=limit,
    )

    if not records:
        logger.info("No new queries captured. Exiting.")
        return {"captured": 0, "sweep_clusters": 0, "flagged_clusters": 0}

    # step 2 -- cluster
    logger.info(f"Clustering {len(records)} captured queries")
    sweep_clusters, flagged = run_clustering(records, min_templates=min_templates)

    # step 3 -- report
    summary = {
        "captured":        len(records),
        "sweep_clusters":  len(sweep_clusters),
        "flagged_clusters": len(flagged),
        "flagged":         [
            {
                "sweep_hash":      c.sweep_hash[:12],
                "refine_hash":     c.refine_hash[:12],
                "template_count":  c.template_count,
                "execution_count": c.query_count,
                "total_credits":   round(c.total_credits, 4),
                "signature":       c.signature,
            }
            for c in flagged
        ],
    }

    logger.info(
        f"Pipeline complete. "
        f"Captured={summary['captured']} "
        f"SweepClusters={summary['sweep_clusters']} "
        f"Flagged={summary['flagged_clusters']}"
    )

    return summary


if __name__ == "__main__":
    import json
    import os

    result = run_pipeline(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )

    print(json.dumps(result, indent=2))
