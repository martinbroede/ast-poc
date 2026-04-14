from opensearchpy import OpenSearch

from ce import (CorrelationEngine, DetectionRule, FieldEqualityConstraint,
                QueryDefinition, StepDefinition)
from core.logic import TriState

_open_search_client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
_mock_engine = CorrelationEngine(client=_open_search_client)
_mock_engine._fetch_events = lambda client, index_name, constraints: _fake_fetch(client, index_name, constraints)


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
        _event(code="21", role="user", user="other", host="X", ts="2026-04-14T10:02:00"),
        _event(code="42", role="user", user="alice", host="host-1", ts="2026-04-14T10:00:00"),
        _event(code="77", role="admin", user="root", host="host-1", ts="2026-04-14T10:01:00"),
        _event(code="21", role="user", user="alice", host="host-9", ts="2026-04-14T10:03:00"),
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
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(name="Q1", constraints={"code": "42"}),
                },
            ),
            StepDefinition(
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

    result_a = engine.evaluate_rule(rule, execution_order=["Q3", "Q1", "Q2"], stop_when_known=False)
    result_b = engine.evaluate_rule(rule, execution_order=["Q1", "Q2", "Q3"], stop_when_known=False)

    assert result_a.state is TriState.TRUE
    assert result_b.state is TriState.TRUE
    assert result_a.triggered is True
    assert result_b.triggered is True


def test_partial_execution_remains_unknown() -> None:
    engine = _mock_engine
    rule = _build_rule()

    result = engine.evaluate_rule(rule, execution_order=["Q3"], stop_when_known=False)

    assert result.state is TriState.UNKNOWN
    assert result.triggered is False


def test_early_stop_when_rule_known_true() -> None:
    engine = _mock_engine
    rule = _build_rule()

    result = engine.evaluate_rule(rule, execution_order=["Q3", "Q1", "Q2"], stop_when_known=True)

    assert result.state is TriState.TRUE
    assert result.executed_queries == ("Q3", "Q1")
