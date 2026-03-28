from core.logic import TriState, evaluate
from core.parser import parse_expr
from core.visualizer import visualize_ast

if __name__ == "__main__":
    expr = parse_expr("(A | B) & (C & (D | E))")

    context = {
        "A": TriState.FALSE,
        "B": TriState.TRUE,
        "C": TriState.TRUE,
        "D": TriState.UNKNOWN,
        "E": TriState.FALSE,
    }

    assert evaluate(expr, context) is TriState.UNKNOWN

    ##################################################

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

    source = "((A | (B & (C | ~D))) & ((E | F) & (G | (H & I))))"
    expr = parse_expr(source)

    print(f"Expression: {source}")
    print ("\n", "*" * 80, "\n")
    print("AST without context:\n")
    print(visualize_ast(expr))
    print ("\n", "*" * 80, "\n")
    print("AST with context:\n")
    print(visualize_ast(expr, context))
    print ("\n", "*" * 80, "\n")
    print(f"Result: {evaluate(expr, context)}")
