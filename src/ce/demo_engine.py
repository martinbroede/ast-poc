import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from opensearchpy import OpenSearch


def build_demo_rule(index_name: str) -> 'DetectionRule':
    return DetectionRule(
        name="demo_rule",
        index_name=index_name,
        steps=[
            StepDefinition(
                max_event_gap=0,
                expression="Q1",
                queries={
                    "Q1": QueryDefinition(
                        name="Q1",
                        constraints={"code": "E04"},
                    ),
                },
            ),
            StepDefinition(
                max_event_gap=60*5,  # 5 minutes
                expression="Q2 | Q3",
                queries={
                    "Q2": QueryDefinition(
                        name="Q2",
                        constraints={"code": "E01"},
                    ),
                    "Q3": QueryDefinition(
                        name="Q3",
                        constraints={"role": "admin"},
                    ),
                },
            ),
            StepDefinition(
                max_event_gap=60 * 60 * 6,  # 6 hours
                expression="Q4",
                queries={
                    "Q4": QueryDefinition(
                        name="Q4",
                        constraints={"code": "E02"},
                    )
                },
            ),
            StepDefinition(
                max_event_gap=60*24,
                expression="Q5",
                queries={
                    "Q5": QueryDefinition(
                        name="Q5",
                        constraints={"code": "not-existing-code"},
                    )
                }
            )
        ],
        constraints=[
            FieldEqualityConstraint(
                left_query="Q1",
                left_field="user",
                right_query="Q2",
                right_field="user",
            ),
            FieldEqualityConstraint(
                left_query="Q1",
                left_field="host",
                right_query="Q3",
                right_field="host",
            ),
            FieldEqualityConstraint(
                left_query="Q3",
                left_field="user",
                right_query="Q4",
                right_field="user",
            ),
        ],
    )


def main() -> None:
    order = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    random.shuffle(order)
    client = OpenSearch(hosts=[{"host": "localhost", "port": 9200}])
    print(f"Order of query execution: {', '.join(order)}")

    engine = CorrelationEngine(client=client)
    rule = build_demo_rule(index_name="events")

    # between 2026-01-15 13:00:00 
    # and 2026-04-15 14:00:00:
    earliest_event_time = datetime(2026, 1, 15, 13, 0, 0)
    latest_event_time = datetime(2026, 4, 15, 14, 0, 0)

    coverage = engine.evaluate_rule_coverage(
        rule,
        earliest_event_time=earliest_event_time,
        latest_event_time=latest_event_time,
        execution_order=order,
        stop_when_known=True,
    )

    print()
    print("Coverage configuration:")
    print(f"  Rule: {rule.name}")
    print(f"  Run frequency (seconds): {rule.run_frequency}")
    print(f"  Earliest event time: {coverage.earliest_event_time.isoformat()}")
    print(f"  Latest event time: {coverage.latest_event_time.isoformat()}")
    print(f"  Executed rule cycles: {len(coverage.runs)}")
    print()
    print("Coverage runs:")

    state_counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
    triggered_count = 0
    total_bindings = 0

    for run in coverage.runs:
        state_counts[run.state.value] = state_counts.get(run.state.value, 0) + 1
        if run.triggered:
            triggered_count += 1
        total_bindings += len(run.bindings)

        run_start = run.run_window_start.isoformat() if run.run_window_start else "unknown"
        run_end = run.run_window_end.isoformat() if run.run_window_end else "unknown"
        executed = ", ".join(run.executed_queries) if run.executed_queries else "<none>"

        print(
            f"  run={run.run_number} "
            f"window=[{run_start}, {run_end}] "
            f"state={run.state.value} "
            f"triggered={run.triggered} "
            f"bindings={len(run.bindings)} "
            f"executed=[{executed}]"
        )

    print()
    print("Possible results summary:")
    print(f"  TRUE runs: {state_counts['TRUE']}")
    print(f"  FALSE runs: {state_counts['FALSE']}")
    print(f"  UNKNOWN runs: {state_counts['UNKNOWN']}")
    print(f"  Triggered runs: {triggered_count}")
    print(f"  Witness bindings (all runs): {total_bindings}")


if __name__ == "__main__":
    SRC_ROOT = Path(__file__).resolve().parents[1]
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
    from ce import (CorrelationEngine, DetectionRule, FieldEqualityConstraint,
                    QueryDefinition, StepDefinition)
    main()
