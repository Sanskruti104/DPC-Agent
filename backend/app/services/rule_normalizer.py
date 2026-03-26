"""
rule_normalizer.py
------------------
Converts raw Rule ORM objects (or dicts) from the database into a normalized,
type-safe format that the downstream validation engine can execute directly.

Responsibilities:
  - Normalize operator strings to canonical symbolic form (e.g., ">" → "gt")
  - Coerce rule values to the correct Python datatype  (str → int / float / list / str)
  - Validate and guard against missing or malformed rule fields

Example Input (raw rule dict from DB):
    {
        "rule_id":  "RULE_A3F2B1",
        "entity":   "customer",
        "field":    "age",
        "operator": ">",
        "value":    "18",
        "action":   "allow"
    }

Example Output (normalized rule):
    {
        "rule_id":  "RULE_A3F2B1",
        "field":    "age",
        "operator": "gt",
        "value":    18,
        "action":   "allow"
    }
"""

from __future__ import annotations

import ast
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Operator map: every valid surface form → canonical name
# ---------------------------------------------------------------------------
_OPERATOR_MAP: dict[str, str] = {
    # Symbolic forms
    ">":   "gt",
    "<":   "lt",
    ">=":  "gte",
    "<=":  "lte",
    "==":  "eq",
    "!=":  "neq",
    # Textual forms (common in LLM output)
    "gt":  "gt",
    "lt":  "lt",
    "gte": "gte",
    "lte": "lte",
    "eq":  "eq",
    "neq": "neq",
    "greater than":          "gt",
    "less than":             "lt",
    "greater than or equal": "gte",
    "less than or equal":    "lte",
    "equal":                 "eq",
    "not equal":             "neq",
    "equals":                "eq",
}


def normalize_operator(raw_operator: str | None) -> str | None:
    """
    Convert a raw operator string to its canonical lower-case symbolic form.

    Args:
        raw_operator: The operator string as stored in the DB, e.g. ``">"`` or
                      ``"greater than"``.

    Returns:
        A canonical operator name such as ``"gt"`` / ``"lt"`` / ``"eq"`` …,
        or ``None`` if the input is blank / unrecognisable.

    Examples:
        >>> normalize_operator(">")
        'gt'
        >>> normalize_operator("GREATER THAN")
        'gt'
        >>> normalize_operator(None)
        None
    """
    if not raw_operator:
        return None

    key = raw_operator.strip().lower()
    result = _OPERATOR_MAP.get(key)

    if result is None:
        logger.warning("Unknown operator '%s' — keeping as-is.", raw_operator)
        return key  # pass through rather than silently drop

    return result


def coerce_value(raw_value: Any) -> int | float | list | str | None:
    """
    Coerce a raw rule value (usually a string from the DB) to the most specific
    Python primitive possible.

    Conversion priority:
        1. ``None`` / empty string → ``None``
        2. Already a numeric type (int/float) → returned unchanged
        3. Already a list → returned unchanged
        4. String that is a valid integer literal → ``int``
        5. String that is a valid float literal   → ``float``
        6. String that looks like a Python list literal → ``list``
        7. Everything else → ``str`` (unchanged)

    Args:
        raw_value: The value as stored in the DB rule record.

    Returns:
        The coerced Python value.

    Examples:
        >>> coerce_value("18")
        18
        >>> coerce_value("18.5")
        18.5
        >>> coerce_value("['A', 'B', 'C']")
        ['A', 'B', 'C']
        >>> coerce_value("pending")
        'pending'
        >>> coerce_value(None)
        None
    """
    # 1. Guard against None / empty
    if raw_value is None:
        return None
    if isinstance(raw_value, str) and not raw_value.strip():
        return None

    # 2. Already the right type — pass through
    if isinstance(raw_value, (int, float, list)):
        return raw_value

    # From here on we treat raw_value as a string
    s = str(raw_value).strip()

    # 3. Try integer
    try:
        return int(s)
    except ValueError:
        pass

    # 4. Try float
    try:
        return float(s)
    except ValueError:
        pass

    # 5. Try list literal  (e.g. "['A','B']" or  "[ 'KYC', 'AML' ]")
    if s.startswith("["):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, SyntaxError):
            logger.warning("Could not parse '%s' as a list literal.", s)

    # 6. Fall back to raw string
    return s


def normalize_rule(rule: dict) -> dict | None:
    """
    Convert a single raw rule dict (from the DB or ORM) into a normalized,
    execution-ready format.

    Required keys in *rule*: ``"field"``, ``"operator"``, ``"value"``
    Optional keys:           ``"rule_id"``, ``"action"``

    Args:
        rule: Raw rule dictionary.

    Returns:
        Normalized rule dict, or ``None`` if the rule is too incomplete to use.

    Example:
        >>> raw = {
        ...     "rule_id":  "RULE_A3F2B1",
        ...     "entity":   "customer",
        ...     "field":    "age",
        ...     "operator": ">",
        ...     "value":    "18",
        ...     "action":   "allow",
        ... }
        >>> normalize_rule(raw)
        {'rule_id': 'RULE_A3F2B1', 'field': 'age', 'operator': 'gt', 'value': 18, 'action': 'allow'}
    """
    if not rule:
        logger.warning("normalize_rule received an empty dict.")
        return None

    field    = rule.get("field")
    operator = rule.get("operator")
    value    = rule.get("value")

    # A rule without field + operator is unusable by the validator
    if not field:
        logger.warning("Rule '%s' has no 'field' — skipping.", rule.get("rule_id", "?"))
        return None
    if not operator:
        logger.warning("Rule '%s' has no 'operator' — skipping.", rule.get("rule_id", "?"))
        return None

    normalized_op    = normalize_operator(operator)
    normalized_value = coerce_value(value)

    return {
        "rule_id":  rule.get("rule_id"),
        "field":    field.strip(),
        "operator": normalized_op,
        "value":    normalized_value,
        "action":   rule.get("action"),
    }


def normalize_rules(rules: list[dict]) -> list[dict]:
    """
    Normalize a list of raw rule dicts, silently dropping any that are invalid.

    Args:
        rules: List of raw rule dicts (e.g. fetched from the DB via
               ``rule_service.get_all_rules()``).

    Returns:
        List of normalized rule dicts ready for the validation engine.

    Example:
        >>> batch = [
        ...     {"rule_id": "RULE_001", "field": "amount",  "operator": ">",  "value": "10000", "action": "flag"},
        ...     {"rule_id": "RULE_002", "field": "status",  "operator": "==", "value": "['KYC_PASS','AML_OK']", "action": "allow"},
        ...     {"rule_id": "RULE_003", "field": "",        "operator": "<",  "value": "100",   "action": "block"},  # bad — no field
        ... ]
        >>> normalize_rules(batch)
        [
            {'rule_id': 'RULE_001', 'field': 'amount', 'operator': 'gt', 'value': 10000, 'action': 'flag'},
            {'rule_id': 'RULE_002', 'field': 'status', 'operator': 'eq', 'value': ['KYC_PASS', 'AML_OK'], 'action': 'allow'},
        ]
    """
    normalized = []
    for raw in rules:
        result = normalize_rule(raw)
        if result is not None:
            normalized.append(result)

    logger.info(
        "Normalized %d / %d rules successfully.", len(normalized), len(rules)
    )
    return normalized


# ---------------------------------------------------------------------------
# Self-test — run directly: python -m app.services.rule_normalizer
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    _SAMPLE_RULES = [
        # Standard integer threshold
        {
            "rule_id":  "RULE_A3F2B1",
            "entity":   "customer",
            "field":    "age",
            "operator": ">",
            "value":    "18",
            "action":   "allow",
        },
        # Float threshold
        {
            "rule_id":  "RULE_B1C2D3",
            "entity":   "transaction",
            "field":    "amount",
            "operator": ">=",
            "value":    "10000.50",
            "action":   "flag",
        },
        # List value  (e.g. allowed status codes)
        {
            "rule_id":  "RULE_C3D4E5",
            "entity":   "customer",
            "field":    "kyc_status",
            "operator": "==",
            "value":    "['VERIFIED', 'PROVISIONED']",
            "action":   "allow",
        },
        # String value
        {
            "rule_id":  "RULE_D4E5F6",
            "entity":   "account",
            "field":    "region",
            "operator": "!=",
            "value":    "SANCTIONED",
            "action":   "block",
        },
        # Textual operator
        {
            "rule_id":  "RULE_E5F6G7",
            "entity":   "loan",
            "field":    "credit_score",
            "operator": "greater than",
            "value":    "700",
            "action":   "approve",
        },
        # Bad rule — missing field (should be dropped)
        {
            "rule_id":  "RULE_BAD_1",
            "entity":   "order",
            "field":    "",
            "operator": "<",
            "value":    "500",
            "action":   "flag",
        },
        # Bad rule — missing operator (should be dropped)
        {
            "rule_id":  "RULE_BAD_2",
            "entity":   "user",
            "field":    "balance",
            "operator": None,
            "value":    "1000",
            "action":   "notify",
        },
    ]

    print("=" * 60)
    print("RULE NORMALIZER — sample run")
    print("=" * 60)
    results = normalize_rules(_SAMPLE_RULES)
    print(json.dumps(results, indent=2))
    print(f"\nInput : {len(_SAMPLE_RULES)} rules")
    print(f"Output: {len(results)} valid normalized rules")
