import ast
import itertools
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

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
class StepWindow:
    """Window used to prefilter events for one step in one execution run."""

    step_index: int
    start: datetime
    end: datetime


@dataclass(frozen=True)
class CorrelationResult:
    """Final outcome of a rule evaluation including state, evidence, and trace."""

    state: TriState
    triggered: bool
    bindings: list[dict[str, dict]]
    executed_queries: tuple[str, ...]
    snapshots: tuple[EvaluationSnapshot, ...]
    run_number: int = 1
    run_window_start: datetime | None = None
    run_window_end: datetime | None = None
    step_windows: tuple[StepWindow, ...] = tuple()


@dataclass(frozen=True)
class CoverageResult:
    """Aggregates results for repeated rule executions covering a time range."""

    first_execution_time: datetime
    earliest_event_time: datetime
    latest_event_time: datetime
    runs: tuple[CorrelationResult, ...]
    logfile_path: str | None = None


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
        self._first_execution_by_rule: dict[str, datetime] = {}

    def evaluate_rule(
        self,
        rule: DetectionRule,
        execution_order: list[str] | None = None,
        stop_when_known: bool = True,
        run_number: int = 1,
        current_time: datetime | None = None,
        first_execution_time: datetime | None = None,
    ) -> CorrelationResult:
        """Evaluates a detection rule incrementally as query results become available.

        The method parses and validates rule expressions, fetches events in execution order,
        re-evaluates the composed expression after each fetch, and optionally stops once the
        tri-state outcome is deterministically known.

        Time-window prefiltering is applied per step according to run frequency and
        cumulative step max-event-gap offsets.
        """

        self._validate_rule_timing(rule)
        if run_number < 1:
            raise ValueError("run_number must be >= 1")

        steps_with_ast = [(step, parse_expr(step.expression)) for step in rule.steps]
        all_queries = self._collect_queries(rule)
        self._validate_steps(steps_with_ast, all_queries)

        anchor_time = self._resolve_first_execution_time(
            rule,
            current_time=current_time,
            first_execution_time=first_execution_time,
        )
        run_start, run_end, step_windows = self._build_step_windows(rule, anchor_time, run_number)
        query_windows = self._build_query_windows(rule, step_windows)

        order = execution_order or list(all_queries.keys())
        self._validate_execution_order(order, all_queries)

        known_events: dict[str, tuple[_EventRecord, ...]] = {name: tuple() for name in all_queries}
        completed: dict[str, bool] = {name: False for name in all_queries}

        snapshots: list[EvaluationSnapshot] = []

        root = self._evaluate_root(steps_with_ast, rule, known_events, completed)
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
            query_window = query_windows[query_name]
            raw_events = self._filter_events_by_window(
                raw_events,
                window_start=query_window.start,
                window_end=query_window.end,
                timestamp_field=rule.timestamp_field,
            )
            known_events[query_name] = self._materialize_events(query_name, raw_events)
            completed[query_name] = True
            executed.append(query_name)

            root = self._evaluate_root(steps_with_ast, rule, known_events, completed)
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
            run_number=run_number,
            run_window_start=run_start,
            run_window_end=run_end,
            step_windows=step_windows,
        )

    def evaluate_rule_coverage(
        self,
        rule: DetectionRule,
        earliest_event_time: datetime,
        latest_event_time: datetime,
        execution_order: list[str] | None = None,
        stop_when_known: bool = True,
        logfile_path: str | None = None,
    ) -> CoverageResult:
        """Runs the rule repeatedly until the full [earliest, latest] range is covered."""

        self._validate_rule_timing(rule)
        earliest = self._normalize_datetime(earliest_event_time)
        latest = self._normalize_datetime(latest_event_time)
        if earliest > latest:
            raise ValueError("earliest_event_time must not be greater than latest_event_time")

        runs: list[CorrelationResult] = []
        run_number = 1

        while True:
            run_result = self.evaluate_rule(
                rule,
                execution_order=execution_order,
                stop_when_known=stop_when_known,
                run_number=run_number,
                current_time=latest,
                first_execution_time=latest,
            )
            runs.append(run_result)

            if logfile_path:
                self._append_coverage_log(logfile_path, rule, run_result)

            if run_result.run_window_start is None or run_result.run_window_start <= earliest:
                break
            run_number += 1

        return CoverageResult(
            first_execution_time=latest,
            earliest_event_time=earliest,
            latest_event_time=latest,
            runs=tuple(runs),
            logfile_path=logfile_path,
        )

    def _fetch_events(self, client: OpenSearch, index_name: str, constraints: dict[str, str | int | float | bool]) -> list[dict]:
        """Fetches raw events for one query using the shared OpenSearch fetch helper."""

        return fetch_events_from_opensearch(client, index_name, constraints)

    @staticmethod
    def _validate_rule_timing(rule: DetectionRule) -> None:
        """Validates timing-related configuration fields required by windowing logic."""

        if rule.run_frequency <= 0:
            raise ValueError("run_frequency must be > 0")

        for idx, step in enumerate(rule.steps):
            if step.max_event_gap < 0:
                raise ValueError(f"steps[{idx}].max_event_gap must be >= 0")

    def _resolve_first_execution_time(
        self,
        rule: DetectionRule,
        current_time: datetime | None,
        first_execution_time: datetime | None,
    ) -> datetime:
        """Resolves and stores the stable first-execution anchor time for one rule."""

        if first_execution_time is not None:
            anchor_time = self._normalize_datetime(first_execution_time)
            self._first_execution_by_rule[rule.name] = anchor_time
            return anchor_time

        cached = self._first_execution_by_rule.get(rule.name)
        if cached is not None:
            return cached

        initial = current_time if current_time is not None else datetime.now(timezone.utc)
        anchor_time = self._normalize_datetime(initial)
        self._first_execution_by_rule[rule.name] = anchor_time
        return anchor_time

    def _build_step_windows(
        self,
        rule: DetectionRule,
        anchor_time: datetime,
        run_number: int,
    ) -> tuple[datetime, datetime, tuple[StepWindow, ...]]:
        """Builds per-step windows for one run using run_frequency and cumulative gaps."""

        run_end = anchor_time - timedelta(seconds=rule.run_frequency * (run_number - 1))
        run_start = run_end - timedelta(seconds=rule.run_frequency)

        cumulative_gap = 0
        windows: list[StepWindow] = []
        for step_idx, step in enumerate(rule.steps):
            if step_idx > 0:
                cumulative_gap += step.max_event_gap
            gap_offset = timedelta(seconds=cumulative_gap)
            windows.append(
                StepWindow(
                    step_index=step_idx,
                    start=run_start - gap_offset,
                    end=run_end - gap_offset,
                )
            )

        return run_start, run_end, tuple(windows)

    @staticmethod
    def _build_query_windows(rule: DetectionRule, step_windows: tuple[StepWindow, ...]) -> dict[str, StepWindow]:
        """Maps each query name to the window of its containing step."""

        query_windows: dict[str, StepWindow] = {}
        for step_idx, step in enumerate(rule.steps):
            for query_name in step.queries:
                query_windows[query_name] = step_windows[step_idx]
        return query_windows

    def _filter_events_by_window(
        self,
        events: list[dict],
        window_start: datetime,
        window_end: datetime,
        timestamp_field: str,
    ) -> list[dict]:
        """Keeps only events with a parseable timestamp inside the inclusive window."""

        filtered: list[dict] = []
        for event in events:
            event_time = self._parse_timestamp(event, timestamp_field)
            if event_time is None:
                continue
            if window_start <= event_time <= window_end:
                filtered.append(event)
        return filtered

    @staticmethod
    def _append_coverage_log(logfile_path: str, rule: DetectionRule, result: CorrelationResult) -> None:
        """Appends one run outcome line to the coverage logfile."""

        log_target = Path(logfile_path)
        log_target.parent.mkdir(parents=True, exist_ok=True)
        executed = ",".join(result.executed_queries) if result.executed_queries else "-"
        run_start = result.run_window_start.isoformat() if result.run_window_start else "unknown"
        run_end = result.run_window_end.isoformat() if result.run_window_end else "unknown"
        line = (
            f"rule={rule.name} run={result.run_number} "
            f"window=[{run_start},{run_end}] "
            f"state={result.state.value} triggered={result.triggered} "
            f"bindings={len(result.bindings)} executed={executed}\n"
        )
        with log_target.open("a", encoding="utf-8") as handle:
            handle.write(line)

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
    ) -> _EvalNodeResult:
        """Evaluates all step expressions and combines them with logical AND semantics."""

        if not steps_with_ast:
            return _EvalNodeResult(state=TriState.FALSE, complete=True, bindings=tuple())

        step_results = [
            self._evaluate_expr(step_ast, known_events, completed, rule)
            for _, step_ast in steps_with_ast
        ]

        current = step_results[0]
        for next_result in step_results[1:]:
            current = self._combine_and(current, next_result, rule)

        return current

    def _evaluate_expr(
        self,
        expr: ast.expr,
        known_events: dict[str, tuple[_EventRecord, ...]],
        completed: dict[str, bool],
        rule: DetectionRule,
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
                left_result = self._evaluate_expr(left, known_events, completed, rule)
                right_result = self._evaluate_expr(right, known_events, completed, rule)
                return self._combine_or(left_result, right_result)

            case ast.BinOp(left=left, op=ast.BitAnd(), right=right):
                left_result = self._evaluate_expr(left, known_events, completed, rule)
                right_result = self._evaluate_expr(right, known_events, completed, rule)
                return self._combine_and(left_result, right_result, rule)

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
    ) -> _EvalNodeResult:
        """Combines two node results with AND semantics via compatibility-aware joins."""

        joined = self._join_compatible_bindings(left.bindings, right.bindings, rule)

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
    ) -> tuple[dict[str, _EventRecord], ...]:
        """Joins binding pairs and keeps only merged candidates that satisfy rule constraints."""

        merged: dict[tuple[tuple[str, str], ...], dict[str, _EventRecord]] = {}
        for left in left_bindings:
            for right in right_bindings:
                combined = self._merge_bindings(left, right)
                if combined is None:
                    continue
                if not self._is_binding_compatible(combined, rule):
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
    ) -> bool:
        """Checks whether one binding obeys value constraints and temporal step sequencing."""

        for constraint in rule.constraints:
            state = self._evaluate_constraint(binding, constraint)
            if state is TriState.FALSE:
                return False

        if not self._respects_consecutive_step_order_and_gap(binding, rule):
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

    def _collect_step_timestamps(
        self,
        binding: dict[str, _EventRecord],
        step: StepDefinition,
        timestamp_field: str,
    ) -> list[datetime] | None:
        """Collects timestamps for bound queries in a step; returns None on unknown timestamps."""

        timestamps: list[datetime] = []
        for query_name in step.queries:
            event = binding.get(query_name)
            if event is None:
                continue
            event_time = self._parse_timestamp(event.payload, timestamp_field)
            if event_time is None:
                return None
            timestamps.append(event_time)
        return timestamps

    def _respects_consecutive_step_order_and_gap(
        self,
        binding: dict[str, _EventRecord],
        rule: DetectionRule,
    ) -> bool:
        """Checks that consecutive steps are ordered and within the step max_event_gap."""

        for step_idx in range(1, len(rule.steps)):
            previous_step = rule.steps[step_idx - 1]
            current_step = rule.steps[step_idx]
            previous_timestamps = self._collect_step_timestamps(binding, previous_step, rule.timestamp_field)
            current_timestamps = self._collect_step_timestamps(binding, current_step, rule.timestamp_field)

            # Unknown timestamps must not prune candidates early.
            if previous_timestamps is None or current_timestamps is None:
                continue
            if not previous_timestamps or not current_timestamps:
                continue

            previous_latest = max(previous_timestamps)
            current_earliest = min(current_timestamps)

            if current_earliest < previous_latest:
                return False

            gap_seconds = (current_earliest - previous_latest).total_seconds()
            if gap_seconds > current_step.max_event_gap:
                return False

        return True

    @staticmethod
    def _normalize_datetime(value: datetime) -> datetime:
        """Normalizes datetime values to naive UTC for consistent comparisons."""

        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _parse_timestamp(event: dict, field: str) -> datetime | None:
        """Parses an ISO timestamp from an event field and returns None if unavailable or invalid."""

        value = CorrelationEngine._resolve_field(event, field)
        if value is None or not isinstance(value, str):
            return None

        try:
            parsed = datetime.fromisoformat(value)
            return CorrelationEngine._normalize_datetime(parsed)
        except ValueError:
            return None
