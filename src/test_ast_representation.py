import ast

import pytest

from core.exceptions import ASTEvaluationError, ASTParsingError
from core.logic import TriState, evaluate
from core.parser import parse_expr


def _dump(node: ast.AST) -> str:
    """
    Helper to dump an AST node as a string for testing purposes.
    """
    return ast.dump(node, include_attributes=False)


def test_evaluate_fails_with_missing_variable() -> None:
    """
    Test that evaluating an AST expression with a missing variable in the context raises a ValueError.
    """
    expr = parse_expr("A & B")
    context = {"A": TriState.TRUE}
    with pytest.raises(ASTEvaluationError):
        evaluate(expr, context) is TriState.UNKNOWN  # type: ignore


def test_evaluate_nested_ast_expression() -> None:
    """
    Test that a nested AST expression is evaluated correctly with a given context.
    """
    expr = parse_expr("(A | B) & (C & (D | E))")

    context = {
        "A": TriState.FALSE,
        "B": TriState.TRUE,
        "C": TriState.TRUE,
        "D": TriState.UNKNOWN,
        "E": TriState.FALSE,
    }

    assert evaluate(expr, context) is TriState.UNKNOWN


def test_parse_supports_symbol_operators() -> None:
    """
    Test that the parser correctly supports the symbol operators '&', '|', and '~' and translates them to the expected AST nodes.
    """
    assert _dump(parse_expr("A & ( ~B | C)")) == _dump(ast.parse("A & ( ~B | C)", mode="eval").body)


@pytest.mark.parametrize(
    "source",
    [
        "",
        "A B",
        "A &",
        "(A | B",
        "A | )",
        "& A",
        "(| A)",
        "A & (B | C))",
    ],
)
def test_invalid_expression_raises_error(source: str) -> None:
    with pytest.raises(ASTParsingError):
        parse_expr(source)


def test_parse_then_evaluate_deep_nested_expression() -> None:
    source = "((A | (B & (C | D))) & ((E | F) & (G | (H & I))))"
    expr = parse_expr(source)

    context = {
        "A": TriState.FALSE,
        "B": TriState.TRUE,
        "C": TriState.FALSE,
        "D": TriState.TRUE,
        "E": TriState.TRUE,
        "F": TriState.FALSE,
        "G": TriState.UNKNOWN,
        "H": TriState.TRUE,
        "I": TriState.UNKNOWN,
    }
    # Left side: FALSE | (TRUE & (FALSE | TRUE)) => TRUE
    # Right side: (TRUE | FALSE) & (UNKNOWN | (TRUE & UNKNOWN)) => TRUE & UNKNOWN => UNKNOWN
    # Total: TRUE & UNKNOWN => UNKNOWN
    assert evaluate(expr, context) is TriState.UNKNOWN

    context = {
        "A": TriState.TRUE,
        "B": TriState.TRUE,
        "C": TriState.FALSE,
        "D": TriState.FALSE,
        "E": TriState.TRUE,
        "F": TriState.FALSE,
        "G": TriState.TRUE,
        "H": TriState.TRUE,
        "I": TriState.FALSE,
    }
    assert evaluate(expr, context) is TriState.TRUE

    # try with different context that evaluates to FALSE
    context = {
        "A": TriState.FALSE,
        "B": TriState.FALSE,
        "C": TriState.FALSE,
        "D": TriState.FALSE,
        "E": TriState.FALSE,
        "F": TriState.FALSE,
        "G": TriState.FALSE,
        "H": TriState.FALSE,
        "I": TriState.FALSE,
    }
    assert evaluate(expr, context) is TriState.FALSE


@pytest.mark.parametrize(
    "expr_str,context,expected",
    [
        (
            "~A & (B | ~C)",
            {
                "A": TriState.TRUE,
                "B": TriState.FALSE,
                "C": TriState.UNKNOWN,
            },
            TriState.FALSE,
        ),
        (
            "~A & (B | ~C)",
            {
                "A": TriState.FALSE,
                "B": TriState.UNKNOWN,
                "C": TriState.FALSE,
            },
            TriState.TRUE,
        ),
        (
            "~A & (~B | ~C)",
            {
                "A": TriState.UNKNOWN,
                "B": TriState.FALSE,
                "C": TriState.TRUE,
            },
            TriState.UNKNOWN,
        ),
    ],
)
def test_ast_with_inverted_variables(expr_str, context, expected):
    expr = parse_expr(expr_str)
    assert evaluate(expr, context) is expected
