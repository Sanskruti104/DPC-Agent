"""
operator_map.py
---------------
Pure execution-engine component — no external dependencies.

Maps canonical operator names (produced by rule_normalizer) to safe,
type-tolerant Python comparison functions.

Usage:
    from app.core.operator_map import get_operator_func

    fn = get_operator_func("gt")
    fn(25, 18)           # → True
    fn("hello", 18)      # → False  (type mismatch, never crashes)
"""

from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exception for unknown operator names
# ---------------------------------------------------------------------------

class UnknownOperatorError(ValueError):
    """Raised when an operator name has no registered handler."""


# ---------------------------------------------------------------------------
# Safe comparison helpers
# ---------------------------------------------------------------------------

def _safe_compare(a: Any, b: Any, op: str) -> bool:
    """
    Attempt a standard Python comparison between *a* and *b*.

    Returns ``False`` (never raises) on:
        - ``TypeError``  — e.g. comparing str to int with ``>``
        - ``ValueError`` — e.g. malformed inputs

    Args:
        a:   Left-hand operand (dataset value).
        b:   Right-hand operand (rule threshold).
        op:  One of ``"eq"``, ``"neq"``, ``"gt"``, ``"lt"``, ``"gte"``,
             ``"lte"``.

    Returns:
        Result of the comparison, or ``False`` on error.
    """
    try:
        if a is None or b is None:
            # None comparisons: only meaningful for eq / neq
            if op == "eq":
                return a is b
            if op == "neq":
                return a is not b
            return False

        if op == "eq":
            return a == b
        if op == "neq":
            return a != b

        # Ordered comparisons require compatible types.
        # Attempt a numeric promotion (str "18" vs int 18).
        a_cmp, b_cmp = _promote_numeric(a, b)

        if op == "gt":
            return a_cmp > b_cmp
        if op == "lt":
            return a_cmp < b_cmp
        if op == "gte":
            return a_cmp >= b_cmp
        if op == "lte":
            return a_cmp <= b_cmp

    except (TypeError, ValueError) as exc:
        logger.debug(
            "Comparison failed (%s %s %s): %s", a, op, b, exc
        )

    return False


def _safe_membership(a: Any, b: Any, *, negate: bool = False) -> bool:
    """
    Check whether *a* is a member of *b*.

    Args:
        a:      Item to look for.
        b:      Container (expected to be a ``list``).
        negate: If ``True``, returns ``not (a in b)``.

    Returns:
        Result of the membership test, or ``False`` if *b* is not iterable.
    """
    if b is None:
        logger.debug("Membership check skipped: container is None")
        return False

    if not isinstance(b, (list, tuple, set, frozenset)):
        logger.warning(
            "Operator 'in'/'not_in' expected a list, got %s. "
            "Wrapping single value into list for a lenient check.",
            type(b).__name__,
        )
        b = [b]

    try:
        result = a in b
        return (not result) if negate else result
    except TypeError as exc:
        logger.debug("Membership check failed (%s in %s): %s", a, b, exc)
        return False


def _promote_numeric(a: Any, b: Any) -> tuple[Any, Any]:
    """
    Try to upcast *a* or *b* so that they share a comparable type.

    Specifically handles the case where one side is a ``str`` that represents
    a number (e.g. the DB stores ``"18"`` but the dataset has the integer
    ``25``).

    Returns the (possibly promoted) pair ``(a, b)``.
    """
    # If both are already the same type, nothing to do
    if type(a) is type(b):
        return a, b

    # str vs numeric — try to cast the string side
    if isinstance(a, str) and isinstance(b, (int, float)):
        try:
            return float(a), float(b)
        except ValueError:
            pass  # not numeric — let the caller handle TypeError

    if isinstance(b, str) and isinstance(a, (int, float)):
        try:
            return float(a), float(b)
        except ValueError:
            pass

    # int vs float — Python handles this natively
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a, b

    # Fall through — caller will get TypeError on comparison, which is fine
    return a, b


# ---------------------------------------------------------------------------
# Operator map
# ---------------------------------------------------------------------------

#: Maps canonical operator names → a ``Callable[[Any, Any], bool]``.
#: All callables are guaranteed to be safe: they return ``False`` on error
#: rather than raising exceptions.
OPERATOR_MAP: dict[str, Callable[[Any, Any], bool]] = {
    # Equality
    "eq":     lambda a, b: _safe_compare(a, b, "eq"),
    "neq":    lambda a, b: _safe_compare(a, b, "neq"),

    # Ordered
    "gt":     lambda a, b: _safe_compare(a, b, "gt"),
    "lt":     lambda a, b: _safe_compare(a, b, "lt"),
    "gte":    lambda a, b: _safe_compare(a, b, "gte"),
    "lte":    lambda a, b: _safe_compare(a, b, "lte"),

    # Membership
    "in":     lambda a, b: _safe_membership(a, b, negate=False),
    "not_in": lambda a, b: _safe_membership(a, b, negate=True),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_operator_func(op: str) -> Callable[[Any, Any], bool]:
    """
    Return the comparison function registered for *op*.

    Args:
        op: Canonical operator name, e.g. ``"gt"``, ``"eq"``, ``"in"``.
            Must be one of the keys defined in :data:`OPERATOR_MAP`.

    Returns:
        A ``Callable[[Any, Any], bool]`` that accepts ``(dataset_value,
        rule_threshold)`` and returns a boolean result.

    Raises:
        UnknownOperatorError: If *op* is ``None``, empty, or not registered.

    Examples:
        >>> get_operator_func("gt")(25, 18)
        True
        >>> get_operator_func("eq")("A", "A")
        True
        >>> get_operator_func("in")("A", ["A", "B"])
        True
        >>> get_operator_func("lte")(5, 10)
        True
        >>> get_operator_func("not_in")("C", ["A", "B"])
        True
    """
    if not op:
        raise UnknownOperatorError(
            "Operator must be a non-empty string, got: {!r}".format(op)
        )

    fn = OPERATOR_MAP.get(op.strip().lower())

    if fn is None:
        supported = ", ".join(sorted(OPERATOR_MAP))
        raise UnknownOperatorError(
            f"Unknown operator {op!r}. Supported operators: {supported}"
        )

    return fn


def list_operators() -> list[str]:
    """Return all supported operator names in sorted order."""
    return sorted(OPERATOR_MAP.keys())


# ---------------------------------------------------------------------------
# Self-test — run directly: python -m app.core.operator_map
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _PASS = "PASS"
    _FAIL = "FAIL"

    def _check(label: str, result: bool, expected: bool) -> None:
        status = _PASS if result == expected else _FAIL
        mark   = "OK" if status == _PASS else "!!"
        print(f"  [{mark}] {label:<45}  got={result}  expected={expected}  -> {status}")

    print("=" * 65)
    print("OPERATOR MAP — self-test")
    print("=" * 65)

    # --- Standard comparisons ---
    print("\nStandard comparisons:")
    _check("gt(25, 18)",               get_operator_func("gt")(25, 18),              True)
    _check("gt(10, 18)",               get_operator_func("gt")(10, 18),              False)
    _check("lt(10, 18)",               get_operator_func("lt")(10, 18),              True)
    _check("gte(18, 18)",              get_operator_func("gte")(18, 18),             True)
    _check("lte(17, 18)",              get_operator_func("lte")(17, 18),             True)
    _check("eq('A', 'A')",             get_operator_func("eq")("A", "A"),            True)
    _check("eq('A', 'B')",             get_operator_func("eq")("A", "B"),            False)
    _check("neq('A', 'B')",            get_operator_func("neq")("A", "B"),           True)

    # --- Membership ---
    print("\nMembership checks:")
    _check("in('A', ['A','B'])",       get_operator_func("in")("A", ["A", "B"]),     True)
    _check("in('C', ['A','B'])",       get_operator_func("in")("C", ["A", "B"]),     False)
    _check("not_in('C', ['A','B'])",   get_operator_func("not_in")("C", ["A", "B"]), True)
    _check("not_in('A', ['A','B'])",   get_operator_func("not_in")("A", ["A", "B"]), False)

    # --- Numeric type promotion ---
    print("\nNumeric promotion (str vs int):")
    _check("gt('25', 18)   str vs int", get_operator_func("gt")("25", 18),           True)
    _check("lte(18, '18.0') int vs str", get_operator_func("lte")(18, "18.0"),       True)

    # --- Edge cases ---
    print("\nEdge cases:")
    _check("eq(None, None)",            get_operator_func("eq")(None, None),          True)
    _check("neq(None, 0)",              get_operator_func("neq")(None, 0),            True)
    _check("gt(None, 10) => False",     get_operator_func("gt")(None, 10),            False)
    _check("gt('hello', 10) => False",  get_operator_func("gt")("hello", 10),         False)
    _check("in('A', None) => False",    get_operator_func("in")("A", None),           False)
    _check("in('A', 'A') single-val",   get_operator_func("in")("A", "A"),            True)

    # --- Unknown operator ---
    print("\nUnknown operator guard:")
    try:
        get_operator_func("between")
        print("  [!!] UnknownOperatorError NOT raised — FAIL")
    except UnknownOperatorError as exc:
        print(f"  [OK] UnknownOperatorError raised correctly: {exc}")

    print(f"\nSupported operators: {list_operators()}")
    print("=" * 65)
