"""
query_builder.py
----------------
Module 4 — Policy-to-Query Compiler.

Converts normalized rule dicts (produced by rule_normalizer) into
Python ``Callable[[dict], bool]`` functions that can be directly applied
to dataset rows without any external dependencies.

Pipeline position:
    rule_normalizer  →  query_builder  →  dataset_validator  →  violation_engine

Usage:
    from app.services.query_builder import build_rule_function, build_rule_functions

    rule = {"rule_id": "RULE_001", "field": "age", "operator": "gt", "value": 18}

    fn = build_rule_function(rule)
    fn({"age": 25})       # True
    fn({"age": 10})       # False
    fn({"name": "Alice"}) # False  (missing field — never crashes)
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from app.core.operator_map import get_operator_func, UnknownOperatorError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type alias for a compiled rule function
# ---------------------------------------------------------------------------

#: A compiled rule: accepts a row ``dict`` and returns ``True`` (violation
#: confirmed / condition met) or ``False`` (condition not met or error).
RuleFunc = Callable[[dict], bool]


# ---------------------------------------------------------------------------
# Safe field accessor
# ---------------------------------------------------------------------------

def _safe_get(row: dict, field: str) -> Any:
    """
    Safely retrieve a field value from a dataset row dict.

    Performs a case-insensitive lookup so that minor column name differences
    (e.g. ``"Amount"`` vs ``"amount"``) are handled gracefully.

    Args:
        row:   A single dataset row represented as ``{column: value}``.
        field: The field name defined in the rule.

    Returns:
        The field value, or ``None`` if:
            - *row* is not a dict
            - the field does not exist (exact or case-insensitive match)

    Examples:
        >>> _safe_get({"age": 25}, "age")
        25
        >>> _safe_get({"Age": 25}, "age")   # case-insensitive fallback
        25
        >>> _safe_get({"age": 25}, "salary")
        None
        >>> _safe_get(None, "age")
        None
    """
    if not isinstance(row, dict) or not field:
        return None

    # 1. Exact match (fast-path)
    if field in row:
        return row[field]

    # 2. Case-insensitive fallback
    field_lower = field.lower()
    for key, val in row.items():
        if isinstance(key, str) and key.lower() == field_lower:
            return val

    return None


# ---------------------------------------------------------------------------
# Core compiler
# ---------------------------------------------------------------------------

def build_rule_function(rule: dict) -> RuleFunc:
    """
    Compile a single normalized rule dict into an executable row-evaluator.

    The returned callable accepts one argument — a dataset row as ``dict`` —
    and returns ``True`` when the rule condition is satisfied, ``False``
    otherwise (including all error / missing-data cases).

    The returned function **never raises**. All errors are caught internally
    and logged at DEBUG level.

    Args:
        rule: A normalized rule dict with keys:
              ``"field"`` (str), ``"operator"`` (str), ``"value"`` (any).
              Optional: ``"rule_id"`` (for logging).

    Returns:
        A ``RuleFunc`` (``Callable[[dict], bool]``).
        If *rule* is malformed (missing field / operator, unknown operator),
        a no-op function that always returns ``False`` is returned, so the
        caller's pipeline is never interrupted.

    Examples:
        >>> fn = build_rule_function({"field": "age", "operator": "gt", "value": 18})
        >>> fn({"age": 25})
        True
        >>> fn({"age": 10})
        False
        >>> fn({"name": "Alice"})   # missing field
        False
    """
    rule_id  = rule.get("rule_id", "<unknown>")
    field    = rule.get("field")
    operator = rule.get("operator")
    value    = rule.get("value")

    # --- Validate required fields up-front, return a safe no-op on failure ---
    if not field:
        logger.warning("[%s] build_rule_function: 'field' is missing — returning no-op.", rule_id)
        return _make_noop(rule_id, reason="missing field")

    if not operator:
        logger.warning("[%s] build_rule_function: 'operator' is missing — returning no-op.", rule_id)
        return _make_noop(rule_id, reason="missing operator")

    # --- Resolve operator function at compile time (not per-row) ---
    try:
        op_fn = get_operator_func(operator)
    except UnknownOperatorError as exc:
        logger.warning("[%s] build_rule_function: %s — returning no-op.", rule_id, exc)
        return _make_noop(rule_id, reason=str(exc))

    # --- Capture variables in closure (avoids late-binding issues) ---
    _rule_id = rule_id
    _field   = field
    _value   = value
    _op_fn   = op_fn

    def rule_fn(row: dict) -> bool:
        """Evaluate the compiled rule against a single dataset row."""
        try:
            row_value = _safe_get(row, _field)

            if row_value is None:
                logger.debug(
                    "[%s] Field '%s' not found or None in row — condition False.",
                    _rule_id, _field,
                )
                return False

            result = _op_fn(row_value, _value)
            logger.debug(
                "[%s] %s(%r, %r) -> %s", _rule_id, operator, row_value, _value, result
            )
            return bool(result)

        except Exception as exc:  # absolute safety net
            logger.debug(
                "[%s] Unexpected error evaluating row — returning False. Error: %s",
                _rule_id, exc,
            )
            return False

    # Attach metadata to the function object for introspection / debugging
    rule_fn.__name__ = f"rule_{rule_id}"
    rule_fn.__doc__  = (
        f"Rule {rule_id}: row['{field}'] {operator} {value!r}"
    )

    return rule_fn


def build_rule_functions(rules: list[dict]) -> list[dict]:
    """
    Compile a list of normalized rule dicts into a list of callable descriptors.

    Useful when the validator needs to iterate over all active rules and apply
    them to every row of a dataset.

    Args:
        rules: List of normalized rule dicts (output of
               ``rule_normalizer.normalize_rules()``).

    Returns:
        A list of descriptors, one per rule::

            [
                {
                    "rule_id":  str,
                    "field":    str,
                    "operator": str,
                    "value":    any,
                    "action":   str | None,
                    "callable": Callable[[dict], bool],
                },
                ...
            ]

    Example:
        >>> rules = [
        ...     {"rule_id": "R1", "field": "amount",  "operator": "gt",  "value": 10000, "action": "flag"},
        ...     {"rule_id": "R2", "field": "status",  "operator": "neq", "value": "ACTIVE", "action": "block"},
        ... ]
        >>> compiled = build_rule_functions(rules)
        >>> compiled[0]["callable"]({"amount": 15000})
        True
        >>> compiled[1]["callable"]({"status": "ACTIVE"})
        False
    """
    compiled = []
    for rule in rules:
        fn = build_rule_function(rule)
        compiled.append({
            "rule_id":  rule.get("rule_id"),
            "field":    rule.get("field"),
            "operator": rule.get("operator"),
            "value":    rule.get("value"),
            "action":   rule.get("action"),
            "callable": fn,
        })

    logger.info("Compiled %d rule function(s).", len(compiled))
    return compiled


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_noop(rule_id: str, reason: str = "") -> RuleFunc:
    """Return a safe always-False function for invalid / incomplete rules."""
    def _noop(row: dict) -> bool:  # noqa: ARG001
        logger.debug("[%s] No-op rule evaluated (reason: %s).", rule_id, reason)
        return False

    _noop.__name__ = f"noop_{rule_id}"
    _noop.__doc__  = f"No-op for rule {rule_id}: {reason}"
    return _noop


# ---------------------------------------------------------------------------
# Self-test — run directly: python -m app.services.query_builder
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    def _check(label: str, result: bool, expected: bool) -> None:
        ok = result == expected
        mark = "OK" if ok else "!!"
        print(f"  [{mark}] {label:<55}  got={result}  expected={expected}")

    print("=" * 70)
    print("QUERY BUILDER -- self-test")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Single rule compilation
    # ------------------------------------------------------------------
    print("\n1. Basic numeric rule  (age > 18):")
    age_rule = {"rule_id": "RULE_001", "field": "age", "operator": "gt", "value": 18}
    fn = build_rule_function(age_rule)
    _check("fn({'age': 25})",              fn({"age": 25}),             True)
    _check("fn({'age': 10})",              fn({"age": 10}),             False)
    _check("fn({'age': 18})  boundary",    fn({"age": 18}),             False)
    _check("fn({'name': 'Alice'}) missing", fn({"name": "Alice"}),       False)
    _check("fn({})            empty row",  fn({}),                       False)
    _check("fn(None)          None row",   fn(None),                     False)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # 2. Float threshold
    # ------------------------------------------------------------------
    print("\n2. Float threshold  (amount >= 10000.50):")
    amt_rule = {"rule_id": "RULE_002", "field": "amount", "operator": "gte", "value": 10000.50}
    fn2 = build_rule_function(amt_rule)
    _check("fn({'amount': 15000})",        fn2({"amount": 15000}),      True)
    _check("fn({'amount': 10000.50})",     fn2({"amount": 10000.50}),   True)
    _check("fn({'amount': 9999.99})",      fn2({"amount": 9999.99}),    False)

    # ------------------------------------------------------------------
    # 3. Equality on string
    # ------------------------------------------------------------------
    print("\n3. String equality  (status == 'ACTIVE'):")
    status_rule = {"rule_id": "RULE_003", "field": "status", "operator": "eq", "value": "ACTIVE"}
    fn3 = build_rule_function(status_rule)
    _check("fn({'status': 'ACTIVE'})",     fn3({"status": "ACTIVE"}),   True)
    _check("fn({'status': 'INACTIVE'})",   fn3({"status": "INACTIVE"}), False)

    # ------------------------------------------------------------------
    # 4. Membership (in)
    # ------------------------------------------------------------------
    print("\n4. Membership  (region in ['US', 'UK', 'EU']):")
    region_rule = {"rule_id": "RULE_004", "field": "region", "operator": "in",
                   "value": ["US", "UK", "EU"]}
    fn4 = build_rule_function(region_rule)
    _check("fn({'region': 'US'})",         fn4({"region": "US"}),       True)
    _check("fn({'region': 'CN'})",         fn4({"region": "CN"}),       False)

    # ------------------------------------------------------------------
    # 5. Case-insensitive field lookup
    # ------------------------------------------------------------------
    print("\n5. Case-insensitive field lookup:")
    _check("fn({'Age': 25}) capital A",    fn({"Age": 25}),             True)
    _check("fn({'AGE': 10}) all caps",     fn({"AGE": 10}),             False)

    # ------------------------------------------------------------------
    # 6. Type mismatch (operator layer handles it)
    # ------------------------------------------------------------------
    print("\n6. Type mismatch handled safely:")
    _check("fn({'age': 'old'}) str vs int", fn({"age": "old"}),          False)
    _check("fn({'age': None})",              fn({"age": None}),           False)

    # ------------------------------------------------------------------
    # 7. Invalid rule → no-op function
    # ------------------------------------------------------------------
    print("\n7. Invalid / incomplete rules -> no-op (always False):")
    noop1 = build_rule_function({"rule_id": "BAD_1"})                        # no field
    noop2 = build_rule_function({"rule_id": "BAD_2", "field": "age"})        # no operator
    noop3 = build_rule_function({"rule_id": "BAD_3", "field": "age",
                                  "operator": "between", "value": 18})       # unknown op
    _check("no-op (no field)     {'age': 25}",    noop1({"age": 25}),        False)
    _check("no-op (no operator)  {'age': 25}",    noop2({"age": 25}),        False)
    _check("no-op (unknown op)   {'age': 25}",    noop3({"age": 25}),        False)

    # ------------------------------------------------------------------
    # 8. Batch compile
    # ------------------------------------------------------------------
    print("\n8. Batch compilation  (build_rule_functions):")
    batch = [
        {"rule_id": "B1", "field": "amount",  "operator": "gt",  "value": 5000,    "action": "flag"},
        {"rule_id": "B2", "field": "country", "operator": "neq", "value": "SAFE",  "action": "block"},
        {"rule_id": "B3", "field": "score",   "operator": "lte", "value": 300,     "action": "review"},
    ]
    compiled = build_rule_functions(batch)
    _check("B1 callable({'amount': 9000})",    compiled[0]["callable"]({"amount": 9000}),    True)
    _check("B1 callable({'amount': 1000})",    compiled[0]["callable"]({"amount": 1000}),    False)
    _check("B2 callable({'country': 'RISKY'})", compiled[1]["callable"]({"country": "RISKY"}), True)
    _check("B3 callable({'score': 250})",      compiled[2]["callable"]({"score": 250}),      True)

    print("\n" + "=" * 70)
    print("Self-test complete.")
    print("=" * 70)
