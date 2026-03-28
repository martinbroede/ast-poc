from core.logic import TriState, evaluate
from core.parser import parse_expr, visualize_ast

if __name__ == "__main__":
    expr = parse_expr("(A | B) & (C & (D | E))")

    context = {
        "A": TriState.FALSE,
        "B": TriState.TRUE,
        "C": TriState.TRUE,
        "D": TriState.UNKNOWN,
        "E": TriState.FALSE,
    }

    context = {
        "A": TriState.UNKNOWN,
        "B": TriState.TRUE,
        "C": TriState.FALSE,
        "D": TriState.TRUE,
        "E": TriState.TRUE,
        "F": TriState.UNKNOWN,
        "G": TriState.FALSE,
        "H": TriState.UNKNOWN,
        "I": TriState.TRUE,
    }

    source = "((A | (B & (C | D))) & ((E | F) & (G | (H & I))))"
    expr = parse_expr(source)

    # Left side: FALSE | (TRUE & (FALSE | TRUE)) => TRUE
    # Right side: (TRUE | FALSE) & (UNKNOWN | (TRUE & UNKNOWN)) => TRUE & UNKNOWN => UNKNOWN
    # Total: TRUE & UNKNOWN => UNKNOWN
    # assert evaluate(expr, context) is TriState.UNKNOWN

    result = evaluate(expr, context)
    print(visualize_ast(expr))
    print(visualize_ast(expr, context))
    print(f"Result: {result}")
