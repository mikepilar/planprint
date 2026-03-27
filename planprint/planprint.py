import sqlglot
import sqlglot.expressions as exp
import hashlib
import json


def extract_join_topology(sql: str, dialect: str = "snowflake") -> dict:
    """
    Parses SQL and extracts the structural join topology.
    Returns a dict representing the canonical plan skeleton
    for the sweep pass.
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)

    topology = {
        "tables": [],
        "joins": [],
    }

    # extract all table references, strip aliases and schema/db prefixes
    for table in ast.find_all(exp.Table):
        topology["tables"].append(table.name.lower())

    # extract join structure
    for join in ast.find_all(exp.Join):
        join_entry = {
            "type": join.args.get("kind", "inner").lower() if join.args.get("kind") else "inner",
            "table": join.this.name.lower() if isinstance(join.this, exp.Table) else None,
            "condition": normalize_join_condition(join),
        }
        topology["joins"].append(join_entry)

    # sort tables and joins for canonical ordering
    topology["tables"] = sorted(topology["tables"])
    topology["joins"] = sorted(topology["joins"], key=lambda j: (j["type"], j["table"] or ""))

    return topology


def normalize_join_condition(join: exp.Join) -> str | None:
    """
    Extracts and normalizes the ON condition of a join.
    Sorts both sides of each equality so a.id = b.id and
    b.id = a.id produce the same string.
    """
    on = join.args.get("on")
    if not on:
        return None

    conditions = []
    for eq in on.find_all(exp.EQ):
        sides = sorted([
            eq.left.sql(dialect="snowflake").lower(),
            eq.right.sql(dialect="snowflake").lower(),
        ])
        conditions.append(f"{sides[0]} = {sides[1]}")

    return " and ".join(sorted(conditions))


def generate_sweep_fingerprint(sql: str, dialect: str = "snowflake") -> tuple[str, dict]:
    """
    Returns the sweep fingerprint hash and the topology dict
    that produced it. Storing both lets us debug and inspect
    what the fingerprint actually represents.
    """
    topology = extract_join_topology(sql, dialect=dialect)
    canonical = json.dumps(topology, sort_keys=True)
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return fingerprint, topology


def generate_query_template_hash(sql: str, dialect: str = "snowflake") -> str:
    """
    Identifies 'same query, different parameters' by stripping
    literals only. Preserves structure including SELECT list.
    Uses sqlglot's anonymous replacement rather than regex.
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)

    # replace all literals with a placeholder
    for literal in ast.find_all(exp.Literal):
        literal.replace(exp.Anonymous(this="?", expressions=[]))

    normalized = ast.sql(dialect="snowflake").lower().strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

def extract_computation_signature(sql: str, dialect: str = "snowflake") -> dict:
    """
    Extracts the computation pattern from the SELECT list.
    Used for the refine pass to sub-cluster within a sweep group.
    Captures aggregations, window functions, and GROUP BY structure.
    Ignores simple column projections.
    """
    ast = sqlglot.parse_one(sql, dialect=dialect)

    signature = {
        "aggregations": [],
        "window_functions": [],
        "group_by": [],
        "has_distinct": False,
    }

    # check for DISTINCT
    select = ast.find(exp.Select)
    if select and select.args.get("distinct"):
        signature["has_distinct"] = True

    # extract aggregation functions
    # but skip any that appear inside a window function -- those get
    # captured separately and we don't want to double count them
    window_nodes = set(id(node) for node in ast.find_all(exp.Window))

    for agg in ast.find_all(exp.AggFunc):
        # walk up the tree to check if this agg lives inside a window
        inside_window = False
        parent = agg.parent
        while parent:
            if isinstance(parent, exp.Window):
                inside_window = True
                break
            parent = parent.parent

        if not inside_window:
            signature["aggregations"].append(
                type(agg).__name__.lower()
            )

    # extract window functions -- capture function type, partition by,
    # and order by columns since these affect the computation pattern
    for window in ast.find_all(exp.Window):
        window_entry = {
            "function": type(window.this).__name__.lower(),
            "partition_by": sorted([
                col.sql(dialect="snowflake").lower()
                for col in window.args.get("partition_by", [])
            ]),
            "order_by": [
                col.sql(dialect="snowflake").lower()
                for col in window.args.get("order", [])
            ],
        }
        signature["window_functions"].append(window_entry)

    # extract GROUP BY columns
    for group in ast.find_all(exp.Group):
        signature["group_by"] = sorted([
            col.sql(dialect="snowflake").lower()
            for col in group.expressions
        ])

    # sort aggregations for canonical ordering
    signature["aggregations"] = sorted(signature["aggregations"])
    signature["window_functions"] = sorted(
        signature["window_functions"],
        key=lambda w: (w["function"], str(w["partition_by"]))
    )

    return signature


def generate_refine_fingerprint(sql: str, dialect: str = "snowflake") -> tuple[str, dict]:
    """
    Returns the refine fingerprint hash and the computation
    signature that produced it. Combined with the sweep fingerprint,
    this gives you the full sub-cluster identity for a query.
    """
    signature = extract_computation_signature(sql, dialect=dialect)
    canonical = json.dumps(signature, sort_keys=True)
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return fingerprint, signature


def get_full_fingerprint(sql: str, dialect: str = "snowflake") -> dict:
    """
    Returns both fingerprints and their source signatures
    in a single call. This is what gets written to the
    query_fingerprints table per captured query.
    """
    sweep_hash, topology = generate_sweep_fingerprint(sql, dialect=dialect)
    refine_hash, signature = generate_refine_fingerprint(sql, dialect=dialect)
    template_hash = generate_query_template_hash(sql, dialect=dialect)

    return {
        "template_hash":   template_hash,
        "sweep_hash":      sweep_hash,
        "refine_hash":     refine_hash,
        "topology":        topology,
        "signature":       signature,
    }