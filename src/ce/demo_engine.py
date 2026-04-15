import random
import sys
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

    result = engine.evaluate_rule(
        rule,
        execution_order=order,
        stop_when_known=True,
    )

    msg = f"""Rule: {rule.name}
    Final state: {result.state}
    Triggered: {result.triggered}"""
    print(msg)
    print(f"Executed queries: {', '.join(result.executed_queries) if result.executed_queries else '<none>'}")
    print()
    print("Snapshots:")
    for idx, snapshot in enumerate(result.snapshots):
        executed = ", ".join(snapshot.executed_queries) if snapshot.executed_queries else "<none>"
        print(f"  {idx}. state={snapshot.state}, executed=[{executed}], candidates={snapshot.candidate_count}")
    print()
    print(f"Witness bindings: {len(result.bindings)}")


if __name__ == "__main__":
    SRC_ROOT = Path(__file__).resolve().parents[1]
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
    from ce import (CorrelationEngine, DetectionRule, FieldEqualityConstraint,
                    QueryDefinition, StepDefinition)
    main()
