import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import snowflake.connector
from snowflake.connector import DictCursor

from planprint.fingerprint import get_full_fingerprint
from planprint.cluster import QueryRecord

logger = logging.getLogger(__name__)


def get_connection(
    user: str,
    password: str,
    account: str,
    warehouse: str,
    role: Optional[str] = None,
) -> snowflake.connector.SnowflakeConnection:
    params = {
        "user":      user,
        "password":  password,
        "account":   account,
        "warehouse": warehouse,
        "database":  "SNOWFLAKE",
        "schema":    "ACCOUNT_USAGE",
    }
    if role:
        params["role"] = role

    return snowflake.connector.connect(**params)


def fetch_recent_queries(
    conn: snowflake.connector.SnowflakeConnection,
    lookback_hours: int = 23,
    min_execution_ms: int = 1000,
    limit: int = 500,
) -> list[dict]:
    """
    Pulls recent SELECT queries from ACCOUNT_USAGE.QUERY_HISTORY.
    Keeps the lookback window under 24 hours by default to ensure
    query profiles are still accessible via GET_QUERY_OPERATOR_STATS.
    """
    since = datetime.utcnow() - timedelta(hours=lookback_hours)

    sql = """
        SELECT
            q.query_id
          , q.query_text
          , q.warehouse_name
          , q.warehouse_size
          , q.execution_time
          , q.bytes_scanned
          , q.start_time
          , w.credits_used_compute
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY w
            ON  w.warehouse_name = q.warehouse_name
            AND w.start_time <= q.start_time
            AND w.end_time    >= q.start_time
        WHERE
            q.execution_status = 'SUCCESS'
            AND q.query_type = 'SELECT'
            AND q.start_time >= %(since)s
            AND q.execution_time >= %(min_execution_ms)s
        ORDER BY q.execution_time DESC
        LIMIT %(limit)s
    """

    cur = conn.cursor(DictCursor)
    cur.execute(sql, {
        "since":            since,
        "min_execution_ms": min_execution_ms,
        "limit":            limit,
    })
    rows = cur.fetchall()
    cur.close()

    return rows


def fetch_query_plan(
    conn: snowflake.connector.SnowflakeConnection,
    query_id: str,
) -> Optional[list[dict]]:
    """
    Fetches the execution plan for a single query via
    GET_QUERY_OPERATOR_STATS. Returns None if the profile
    has expired or the query_id is not found.
    """
    try:
        cur = conn.cursor(DictCursor)
        cur.execute(
            f"SELECT * FROM TABLE(GET_QUERY_OPERATOR_STATS('{query_id}'))"
        )
        rows = cur.fetchall()
        cur.close()
        return rows if rows else None
    except Exception as e:
        logger.warning(f"Plan fetch failed for {query_id}: {e}")
        return None


def extract_projected_columns(sql: str) -> list[str]:
    """
    Extracts the raw SELECT list for storage. Used later by the
    materialization suggestion step to determine which columns
    a suggested view needs to expose.
    Does not normalize -- stores what the query actually requested.
    """
    import sqlglot
    import sqlglot.expressions as exp

    try:
        ast = sqlglot.parse_one(sql, dialect="snowflake")
        select = ast.find(exp.Select)
        if not select:
            return []

        columns = []
        for expr in select.expressions:
            columns.append(expr.sql(dialect="snowflake"))

        return columns
    except Exception:
        return []


def already_captured(
    conn: snowflake.connector.SnowflakeConnection,
    query_id: str,
) -> bool:
    """
    Checks whether a query_id has already been written to the
    fingerprints table. Prevents duplicate captures on overlapping
    capture runs.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(1)
        FROM planprint.public.query_fingerprints
        WHERE query_id = %s
        """,
        (query_id,)
    )
    count = cur.fetchone()[0]
    cur.close()
    return count > 0


def write_fingerprint(
    conn: snowflake.connector.SnowflakeConnection,
    record: QueryRecord,
    plan_json: Optional[list[dict]],
    projected_columns: list[str],
) -> None:
    """
    Writes a single fingerprinted query to the persistence table.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO planprint.public.query_fingerprints (
            query_id
          , template_hash
          , sweep_hash
          , refine_hash
          , warehouse_name
          , query_text
          , topology_json
          , signature_json
          , plan_json
          , projected_columns
          , execution_time_ms
          , bytes_scanned
          , credits_used
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
            %s, %s, %s
        )
        """,
        (
            record.query_id,
            record.template_hash,
            record.sweep_hash,
            record.refine_hash,
            record.warehouse_name,
            record.query_text,
            json.dumps(record.topology),
            json.dumps(record.signature),
            json.dumps(plan_json) if plan_json else None,
            json.dumps(projected_columns),
            record.execution_time_ms,
            record.bytes_scanned,
            record.credits_used,
        )
    )
    cur.close()


def run_capture(
    user: str,
    password: str,
    account: str,
    warehouse: str,
    role: Optional[str] = None,
    lookback_hours: int = 23,
    min_execution_ms: int = 1000,
    limit: int = 500,
) -> list[QueryRecord]:
    """
    Full capture pipeline. Connects to Snowflake, pulls recent queries,
    fingerprints each one, and writes results to the persistence table.
    Returns the list of QueryRecords for downstream clustering.
    """
    conn = get_connection(user, password, account, warehouse, role)

    logger.info("Fetching recent queries from QUERY_HISTORY")
    rows = fetch_recent_queries(conn, lookback_hours, min_execution_ms, limit)
    logger.info(f"Found {len(rows)} queries to process")

    records = []

    for row in rows:
        query_id = row["QUERY_ID"]

        # skip anything already in the table
        if already_captured(conn, query_id):
            logger.debug(f"Skipping already captured query {query_id}")
            continue

        query_text = row["QUERY_TEXT"]

        # fingerprint -- skip if sqlglot can't parse it
        try:
            fp = get_full_fingerprint(query_text)
        except Exception as e:
            logger.warning(f"Fingerprint failed for {query_id}: {e}")
            continue

        plan_json = fetch_query_plan(conn, query_id)
        projected_columns = extract_projected_columns(query_text)

        record = QueryRecord(
            query_id=query_id,
            template_hash=fp["template_hash"],
            sweep_hash=fp["sweep_hash"],
            refine_hash=fp["refine_hash"],
            query_text=query_text,
            topology=fp["topology"],
            signature=fp["signature"],
            warehouse_name=row.get("WAREHOUSE_NAME"),
            execution_time_ms=row.get("EXECUTION_TIME"),
            bytes_scanned=row.get("BYTES_SCANNED"),
            credits_used=row.get("CREDITS_USED_COMPUTE"),
        )

        write_fingerprint(conn, record, plan_json, projected_columns)
        records.append(record)

        logger.info(f"Captured {query_id} sweep={record.sweep_hash[:8]} refine={record.refine_hash[:8]}")

    conn.close()
    logger.info(f"Capture complete. Processed {len(records)} new queries.")

    return records
