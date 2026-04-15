import ast
import itertools
from dataclasses import dataclass
from datetime import datetime

from opensearchpy import OpenSearch

from core.logic import TriState
from core.parser import parse_expr
from qe.fetch import fetch_events_from_opensearch


@dataclass(frozen=True)
class QueryDefinition:
    """Defines one named event query and its filter constraints."""

    name: str
    constraints: dict[str, str | int | float | bool]


@dataclass(frozen=True)
class StepDefinition:
    """Describes a correlation step expression and the queries it depends on."""

    expression: str
    queries: dict[str, QueryDefinition]
    max_event_gap: int = 0


@dataclass(frozen=True)
class FieldEqualityConstraint:
    """Represents a cross-query equality check between two event fields."""

    left_query: str
    left_field: str
    right_query: str
    right_field: str


@dataclass(frozen=True)
class DetectionRule:
    """Bundles the full rule definition used for correlation evaluation."""

    name: str
    steps: list[StepDefinition]
    constraints: list[FieldEqualityConstraint]
    run_frequency: int = 60 * 5
    index_name: str = "events"
    timestamp_field: str = "timestamp"


@dataclass(frozen=True)
class EvaluationSnapshot:
    """Captures intermediate evaluation status after each query execution step."""

    state: TriState
    executed_queries: tuple[str, ...]
    candidate_count: int


@dataclass(frozen=True)
class CorrelationResult:
    """Final outcome of a rule evaluation including state, evidence, and trace."""

    state: TriState
    triggered: bool
    bindings: list[dict[str, dict]]
    executed_queries: tuple[str, ...]
    snapshots: tuple[EvaluationSnapshot, ...]


@dataclass(frozen=True)
class _EventRecord:
    """Internal wrapper for an event payload with a deterministic unique id."""

    uid: str
    payload: dict


@dataclass(frozen=True)
class _EvalNodeResult:
    """Internal evaluation output for an expression node and its candidate bindings."""

    state: TriState
    complete: bool
    bindings: tuple[dict[str, _EventRecord], ...]


class CorrelationEngine:
    def __init__(self, client: OpenSearch):
        self._client = client
        self._fetch_events

    def evaluate_rule(
        self,
        rule: DetectionRule,
        execution_order: list[str] | None = None,
        stop_when_known: bool = True,
    ) -> CorrelationResult:
        """Evaluates a detection rule incrementally as query results become available.

        The method parses and validates rule expressions, fetches events in execution order,
        re-evaluates the composed expression after each fetch, and optionally stops once the
        tri-state outcome is deterministically known.
        """

        steps_with_ast = [(step, parse_expr(step.expression)) for step in rule.steps]
        all_queries = self._collect_queries(rule)
        self._validate_steps(steps_with_ast, all_queries)

        order = execution_order or list(all_queries.keys())
        self._validate_execution_order(order, all_queries)

        known_events: dict[str, tuple[_EventRecord, ...]] = {name: tuple() for name in all_queries}
        completed: dict[str, bool] = {name: False for name in all_queries}

        step_map = self._build_step_map(rule)
        snapshots: list[EvaluationSnapshot] = []

        root = self._evaluate_root(steps_with_ast, rule, known_events, completed, step_map)
        snapshots.append(
            EvaluationSnapshot(
                state=root.state,
                executed_queries=tuple(),
                candidate_count=len(root.bindings),
            )
        )

        executed: list[str] = []
        for query_name in order:
            query = all_queries[query_name]
            raw_events = self._fetch_events(
                self._client,
                rule.index_name,
                query.constraints,
            )
            known_events[query_name] = self._materialize_events(query_name, raw_events)
            completed[query_name] = True
            executed.append(query_name)

            root = self._evaluate_root(steps_with_ast, rule, known_events, completed, step_map)
            snapshots.append(
                EvaluationSnapshot(
                    state=root.state,
                    executed_queries=tuple(executed),
                    candidate_count=len(root.bindings),
                )
            )

            if stop_when_known and root.state != TriState.UNKNOWN:
                break

        result_bindings = [
            {query_name: event.payload for query_name, event in binding.items()}
            for binding in root.bindings
        ]

        return CorrelationResult(
            state=root.state,
            triggered=root.state is TriState.TRUE,
            bindings=result_bindings,
            executed_queries=tuple(executed),
            snapshots=tuple(snapshots),
        )

    def _fetch_events(self, client: OpenSearch, index_name: str, constraints: dict[str, str | int | float | bool]) -> list[dict]:
        """Fetches raw events for one query using the shared OpenSearch fetch helper."""

        return fetch_events_from_opensearch(client, index_name, constraints)

    @staticmethod
    def _collect_queries(rule: DetectionRule) -> dict[str, QueryDefinition]:
        """Collects unique query definitions from all steps and rejects conflicting duplicates."""

        queries: dict[str, QueryDefinition] = {}
        for step in rule.steps:
            for name, query in step.queries.items():
                if name in queries and queries[name] != query:
                    raise ValueError(f"Query {name!r} is defined multiple times with different constraints")
                queries[name] = query
        return queries

    @staticmethod
    def _build_step_map(rule: DetectionRule) -> dict[str, int]:
        """Builds a query-to-step index map used for temporal ordering checks."""

        step_map: dict[str, int] = {}
        for idx, step in enumerate(rule.steps):
            for query_name in step.queries:
                step_map[query_name] = idx
        return step_map

    @staticmethod
    def _materialize_events(query_name: str, events: list[dict]) -> tuple[_EventRecord, ...]:
        """Converts raw events to internal records with stable per-query identifiers."""

        return tuple(
            _EventRecord(uid=f"{query_name}:{idx}", payload=event)
            for idx, event in enumerate(events)
        )

    @staticmethod
    def _validate_execution_order(order: list[str], queries: dict[str, QueryDefinition]) -> None:
        """Ensures execution order contains only known queries and no duplicates."""

        seen: set[str] = set()
        for query_name in order:
            if query_name not in queries:
                raise ValueError(f"Unknown query in execution order: {query_name!r}")
            if query_name in seen:
                raise ValueError(f"Duplicate query in execution order: {query_name!r}")
            seen.add(query_name)

    @staticmethod
    def _collect_expr_names(expr: ast.expr) -> set[str]:
        """Extracts all query variable names referenced inside an expression AST."""

        names: set[str] = set()
        for node in ast.walk(expr):
            if isinstance(node, ast.Name):
                names.add(node.id)
        return names

    def _validate_steps(self, steps_with_ast: list[tuple[StepDefinition, ast.expr]], all_queries: dict[str, QueryDefinition]) -> None:
        """Validates that step expressions reference only declared queries and all step queries are used."""

        known = set(all_queries)
        for step, step_ast in steps_with_ast:
            expr_names = self._collect_expr_names(step_ast)
            unknown_names = expr_names - known
            if unknown_names:
                raise ValueError(
                    f"Step expression {step.expression!r} references unknown queries: {sorted(unknown_names)}"
                )
            for query_name in step.queries:
                if query_name not in expr_names:
                    raise ValueError(
                        f"Step expression {step.expression!r} does not reference query {query_name!r}"
                    )

    def _evaluate_root(
        self,
        steps_with_ast: list[tuple[StepDefinition, ast.expr]],
        rule: DetectionRule,
        known_events: dict[str, tuple[_EventRecord, ...]],
        completed: dict[str, bool],
        step_map: dict[str, int],
    ) -> _EvalNodeResult:
        """Evaluates all step expressions and combines them with logical AND semantics."""

        if not steps_with_ast:
            return _EvalNodeResult(state=TriState.FALSE, complete=True, bindings=tuple())

        step_results = [
            self._evaluate_expr(step_ast, known_events, completed, rule, step_map)
            for _, step_ast in steps_with_ast
        ]

        current = step_results[0]
        for next_result in step_results[1:]:
            current = self._combine_and(current, next_result, rule, step_map)

        return current

    def _evaluate_expr(
        self,
        expr: ast.expr,
        known_events: dict[str, tuple[_EventRecord, ...]],
        completed: dict[str, bool],
        rule: DetectionRule,
        step_map: dict[str, int],
    ) -> _EvalNodeResult:
        """Recursively evaluates an expression AST into tri-state truth and candidate bindings."""

        match expr:
            case ast.Name(id=query_name):
                bindings = tuple({query_name: event} for event in known_events[query_name])
                if bindings:
                    return _EvalNodeResult(state=TriState.TRUE, complete=completed[query_name], bindings=bindings)
                if completed[query_name]:
                    return _EvalNodeResult(state=TriState.FALSE, complete=True, bindings=tuple())
                return _EvalNodeResult(state=TriState.UNKNOWN, complete=False, bindings=tuple())

            case ast.BinOp(left=left, op=ast.BitOr(), right=right):
                left_result = self._evaluate_expr(left, known_events, completed, rule, step_map)
                right_result = self._evaluate_expr(right, known_events, completed, rule, step_map)
                return self._combine_or(left_result, right_result)

            case ast.BinOp(left=left, op=ast.BitAnd(), right=right):
                left_result = self._evaluate_expr(left, known_events, completed, rule, step_map)
                right_result = self._evaluate_expr(right, known_events, completed, rule, step_map)
                return self._combine_and(left_result, right_result, rule, step_map)

            case ast.UnaryOp(op=ast.Invert()):
                raise ValueError("Unary '~' is not supported in correlation expressions")

            case _:
                raise ValueError(f"Unsupported AST node in correlation rule: {type(expr)!r}")

    @staticmethod
    def _binding_key(binding: dict[str, _EventRecord]) -> tuple[tuple[str, str], ...]:
        """Builds a canonical key for deduplicating equivalent bindings."""

        return tuple(sorted((query_name, event.uid) for query_name, event in binding.items()))

    def _combine_or(self, left: _EvalNodeResult, right: _EvalNodeResult) -> _EvalNodeResult:
        """Combines two node results with OR semantics and merged binding candidates."""

        merged: dict[tuple[tuple[str, str], ...], dict[str, _EventRecord]] = {}
        for binding in itertools.chain(left.bindings, right.bindings):
            merged[self._binding_key(binding)] = binding

        bindings = tuple(merged.values())
        if bindings:
            return _EvalNodeResult(state=TriState.TRUE, complete=left.complete and right.complete, bindings=bindings)
        if left.state is TriState.FALSE and right.state is TriState.FALSE:
            return _EvalNodeResult(state=TriState.FALSE, complete=left.complete and right.complete, bindings=tuple())
        return _EvalNodeResult(state=TriState.UNKNOWN, complete=left.complete and right.complete, bindings=tuple())

    def _combine_and(
        self,
        left: _EvalNodeResult,
        right: _EvalNodeResult,
        rule: DetectionRule,
        step_map: dict[str, int],
    ) -> _EvalNodeResult:
        """Combines two node results with AND semantics via compatibility-aware joins."""

        joined = self._join_compatible_bindings(left.bindings, right.bindings, rule, step_map)

        if joined:
            return _EvalNodeResult(state=TriState.TRUE, complete=left.complete and right.complete, bindings=joined)
        if left.state is TriState.FALSE or right.state is TriState.FALSE:
            return _EvalNodeResult(state=TriState.FALSE, complete=left.complete and right.complete, bindings=tuple())
        if left.complete and right.complete:
            return _EvalNodeResult(state=TriState.FALSE, complete=True, bindings=tuple())
        return _EvalNodeResult(state=TriState.UNKNOWN, complete=False, bindings=tuple())

    def _join_compatible_bindings(
        self,
        left_bindings: tuple[dict[str, _EventRecord], ...],
        right_bindings: tuple[dict[str, _EventRecord], ...],
        rule: DetectionRule,
        step_map: dict[str, int],
    ) -> tuple[dict[str, _EventRecord], ...]:
        """Joins binding pairs and keeps only merged candidates that satisfy rule constraints."""

        merged: dict[tuple[tuple[str, str], ...], dict[str, _EventRecord]] = {}
        for left in left_bindings:
            for right in right_bindings:
                combined = self._merge_bindings(left, right)
                if combined is None:
                    continue
                if not self._is_binding_compatible(combined, rule, step_map):
                    continue
                merged[self._binding_key(combined)] = combined
        return tuple(merged.values())

    @staticmethod
    def _merge_bindings(
        left: dict[str, _EventRecord],
        right: dict[str, _EventRecord],
    ) -> dict[str, _EventRecord] | None:
        """Merges two bindings unless they assign different events to the same query name."""

        merged = dict(left)
        for query_name, event in right.items():
            if query_name in merged and merged[query_name].uid != event.uid:
                return None
            merged[query_name] = event
        return merged

    def _is_binding_compatible(
        self,
        binding: dict[str, _EventRecord],
        rule: DetectionRule,
        step_map: dict[str, int],
    ) -> bool:
        """Checks whether one binding obeys value constraints and temporal step ordering."""

        for constraint in rule.constraints:
            state = self._evaluate_constraint(binding, constraint)
            if state is TriState.FALSE:
                return False

        for left_query, right_query in itertools.combinations(binding.keys(), 2):
            if not self._respects_step_order(
                binding,
                left_query,
                right_query,
                step_map,
                rule.timestamp_field,
            ):
                return False

        return True

    def _evaluate_constraint(
        self,
        binding: dict[str, _EventRecord],
        constraint: FieldEqualityConstraint,
    ) -> TriState:
        """Evaluates a field-equality constraint on a partial or complete binding."""

        left_event = binding.get(constraint.left_query)
        right_event = binding.get(constraint.right_query)
        if left_event is None or right_event is None:
            return TriState.UNKNOWN

        left_value = self._resolve_field(left_event.payload, constraint.left_field)
        right_value = self._resolve_field(right_event.payload, constraint.right_field)
        if left_value is None or right_value is None:
            return TriState.UNKNOWN
        return TriState.TRUE if left_value == right_value else TriState.FALSE

    @staticmethod
    def _resolve_field(event: dict, field: str):
        """Resolves a dotted field path inside a nested event dictionary."""

        current = event
        for part in field.split("."):
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        return current

    def _respects_step_order(
        self,
        binding: dict[str, _EventRecord],
        query_a: str,
        query_b: str,
        step_map: dict[str, int],
        timestamp_field: str,
    ) -> bool:
        """Verifies that events from earlier steps occur before later-step events by timestamp."""

        step_a = step_map[query_a]
        step_b = step_map[query_b]
        if step_a == step_b:
            return True

        earlier_query, later_query = (query_a, query_b) if step_a < step_b else (query_b, query_a)
        earlier_ts = self._parse_timestamp(binding[earlier_query].payload, timestamp_field)
        later_ts = self._parse_timestamp(binding[later_query].payload, timestamp_field)

        # Unknown timestamps must not prune candidates early.
        if earlier_ts is None or later_ts is None:
            return True
        return earlier_ts < later_ts

    @staticmethod
    def _parse_timestamp(event: dict, field: str) -> datetime | None:
        """Parses an ISO timestamp from an event field and returns None if unavailable or invalid."""

        value = CorrelationEngine._resolve_field(event, field)
        if value is None or not isinstance(value, str):
            return None

        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
