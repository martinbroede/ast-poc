from datetime import datetime

from opensearchpy import OpenSearch

from ce import (CorrelationEngine, DetectionRule, FieldEqualityConstraint,
                QueryDefinition, StepDefinition)
from core.logic import TriState

_open_search_client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
_mock_engine = CorrelationEngine(client=_open_search_client)
_mock_engine._fetch_events = lambda client, index_name, constraints: _fake_fetch(client, index_name, constraints)
_ANCHOR = datetime.fromisoformat("2026-04-14T10:05:00")


def _event(code: str, role: str, user: str, host: str, ts: str) -> dict:
    return {
        "code": code,
        "role": role,
        "user": user,
        "host": host,
        "timestamp": ts,
    }


def _fake_fetch(_client, _index_name, constraints):
    dataset = [
        _event(code="21", role="user", user="alice", host="host-9", ts="2026-04-14T10:00:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:00:00"),
        _event(code="77", role="admin", user="root", host="host-1", ts="2026-04-14T10:00:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T09:58:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T09:52:00"),
    ]

    if not constraints:
        return dataset

    matches = []
    for event in dataset:
        if all(event.get(key) == value for key, value in constraints.items()):
            matches.append(event)
    return matches


def _build_rule() -> DetectionRule:
    return DetectionRule(
        name="sample",
        steps=[
            StepDefinition(
                max_event_gap=0,
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
            StepDefinition(
                max_event_gap=300,
                expression="Q2 | Q3",
                queries={
                    "Q2": QueryDefinition(name="Q2", constraints={"code": "21"}),
                    "Q3": QueryDefinition(name="Q3", constraints={"role": "admin"}),
                },
            ),
        ],
        constraints=[
            FieldEqualityConstraint("Q1", "user", "Q2", "user"),
            FieldEqualityConstraint("Q1", "host", "Q3", "host"),
        ],
    )


def test_order_independent_trigger_result() -> None:
    engine = _mock_engine
    rule = _build_rule()

    result_a = engine.evaluate_rule(
        rule,
        execution_order=["Q3", "Q1", "Q2"],
        stop_when_known=False,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )
    result_b = engine.evaluate_rule(
        rule,
        execution_order=["Q1", "Q2", "Q3"],
        stop_when_known=False,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    assert result_a.state is TriState.TRUE
    assert result_b.state is TriState.TRUE
    assert result_a.triggered is True
    assert result_b.triggered is True


def test_partial_execution_remains_unknown() -> None:
    engine = _mock_engine
    rule = _build_rule()

    result = engine.evaluate_rule(
        rule,
        execution_order=["Q3"],
        stop_when_known=False,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    assert result.state is TriState.UNKNOWN
    assert result.triggered is False


def test_early_stop_when_rule_known_true() -> None:
    engine = _mock_engine
    rule = _build_rule()

    result = engine.evaluate_rule(
        rule,
        execution_order=["Q3", "Q1", "Q2"],
        stop_when_known=True,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    assert result.state is TriState.TRUE
    assert result.executed_queries == ("Q3", "Q1")


def test_window_prefilter_shifts_with_run_number() -> None:
    engine = CorrelationEngine(client=_open_search_client)
    dataset = [
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:03:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T09:58:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T09:52:00"),
    ]
    engine._fetch_events = lambda client, index_name, constraints: [
        event
        for event in dataset
        if all(event.get(key) == value for key, value in constraints.items())
    ]

    rule = DetectionRule(
        name="window_shift",
        run_frequency=300,
        steps=[
            StepDefinition(
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
        ],
        constraints=[],
    )

    run_1 = engine.evaluate_rule(
        rule,
        stop_when_known=False,
        run_number=1,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )
    run_2 = engine.evaluate_rule(
        rule,
        stop_when_known=False,
        run_number=2,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    run_1_timestamps = {binding["Q1"]["timestamp"] for binding in run_1.bindings}
    run_2_timestamps = {binding["Q1"]["timestamp"] for binding in run_2.bindings}

    assert run_1_timestamps == {"2026-04-14T10:03:00"}
    assert run_2_timestamps == {"2026-04-14T09:58:00"}


def test_post_processing_filters_reverse_step_order() -> None:
    engine = CorrelationEngine(client=_open_search_client)
    dataset = [
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:00:00"),
        _event(code="21", role="user", user="alice", host="host-1", ts="2026-04-14T09:58:00"),
    ]
    engine._fetch_events = lambda client, index_name, constraints: [
        event
        for event in dataset
        if all(event.get(key) == value for key, value in constraints.items())
    ]

    rule = DetectionRule(
        name="reverse_order",
        run_frequency=300,
        steps=[
            StepDefinition(
                max_event_gap=0,
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
            StepDefinition(
                max_event_gap=300,
                expression="Q2",
                queries={
                    "Q2": QueryDefinition(name="Q2", constraints={"code": "21"}),
                },
            ),
        ],
        constraints=[],
    )

    result = engine.evaluate_rule(
        rule,
        execution_order=["Q1", "Q2"],
        stop_when_known=False,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    assert result.state is TriState.FALSE
    assert result.triggered is False
    assert result.bindings == []


def test_coverage_runs_until_earliest_and_logs(tmp_path) -> None:
    engine = CorrelationEngine(client=_open_search_client)
    dataset = [
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:03:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T09:58:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T09:48:00"),
    ]
    engine._fetch_events = lambda client, index_name, constraints: [
        event
        for event in dataset
        if all(event.get(key) == value for key, value in constraints.items())
    ]

    rule = DetectionRule(
        name="coverage",
        run_frequency=300,
        steps=[
            StepDefinition(
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
        ],
        constraints=[],
    )

    logfile = tmp_path / "coverage.log"
    coverage = engine.evaluate_rule_coverage(
        rule,
        earliest_event_time=datetime.fromisoformat("2026-04-14T09:46:00"),
        latest_event_time=_ANCHOR,
        stop_when_known=False,
        logfile_path=str(logfile),
    )

    assert len(coverage.runs) == 4
    assert coverage.runs[0].run_number == 1
    assert coverage.runs[-1].run_number == 4
    assert logfile.exists()
    lines = logfile.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 4
    assert "run=1" in lines[0]
    assert "run=4" in lines[-1]


def test_equal_timestamps_between_steps_are_allowed() -> None:
    engine = CorrelationEngine(client=_open_search_client)
    dataset = [
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:00:00"),
        _event(code="21", role="user", user="alice", host="host-1", ts="2026-04-14T10:00:00"),
    ]
    engine._fetch_events = lambda client, index_name, constraints: [
        event
        for event in dataset
        if all(event.get(key) == value for key, value in constraints.items())
    ]

    rule = DetectionRule(
        name="equal_timestamps",
        run_frequency=300,
        steps=[
            StepDefinition(
                max_event_gap=0,
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
            StepDefinition(
                max_event_gap=0,
                expression="Q2",
                queries={
                    "Q2": QueryDefinition(name="Q2", constraints={"code": "21"}),
                },
            ),
        ],
        constraints=[],
    )

    result = engine.evaluate_rule(
        rule,
        execution_order=["Q2", "Q1"],
        stop_when_known=False,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    assert result.state is TriState.TRUE
    assert result.triggered is True
    assert len(result.bindings) == 1


def test_missing_timestamps_are_ignored() -> None:
    engine = CorrelationEngine(client=_open_search_client)
    dataset = [
        {"code": "42", "role": "user", "user": "alice", "host": "host-1"},
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:03:00"),
    ]
    engine._fetch_events = lambda client, index_name, constraints: [
        event
        for event in dataset
        if all(event.get(key) == value for key, value in constraints.items())
    ]

    rule = DetectionRule(
        name="missing_timestamps",
        run_frequency=300,
        steps=[
            StepDefinition(
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
        ],
        constraints=[],
    )

    result = engine.evaluate_rule(
        rule,
        stop_when_known=False,
        current_time=_ANCHOR,
        first_execution_time=_ANCHOR,
    )

    assert result.state is TriState.TRUE
    assert result.triggered is True
    assert len(result.bindings) == 1
    assert result.bindings[0]["Q1"]["timestamp"] == "2026-04-14T10:03:00"
