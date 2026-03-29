from core.logic import TriState
from core.parser import parse_expr
from core.visualizer import visualize_ast

_TREE_WITHOUT_CONTEXT_EXPECTED = \
    """
               [AND]
                │
      ┌─────────┴─────────┐
    [OR]                [AND]
      │                   │
  ┌───┴────┐          ┌───┴────┐
[NOT]    [AND]      [OR]     [OR]
  │        │          │        │
  │     ┌──┴──┐     ┌─┴─┐   ┌──┴──┐
A       B   [OR]    E   F   G   [AND]
              │                   │
            ┌─┴─┐               ┌─┴─┐
            C   D               H   I
"""

_TREE_WITH_CONTEXT_EXPECTED = \
    """
                          [AND]
                            │
          ┌─────────────────┴─────────────────┐
        [OR]                                [AND]
          │                                   │
   ┌──────┴──────┐                    ┌───────┴────────┐
 [NOT]         [AND]                [OR]             [OR]
   │             │                    │                │
   │        ┌────┴─────┐           ┌──┴───┐       ┌────┴────┐
UNKNOWN   TRUE       [OR]        TRUE   FALSE   TRUE      [AND]
                       │                                    │
                   ┌───┴───┐                             ┌──┴───┐
                 FALSE   FALSE                         TRUE   FALSE
"""


def test_visualizer():
    source = "((~A | (B & (C | D))) & ((E | F) & (G | (H & I))))"
    context = {
        "A": TriState.UNKNOWN,
        "B": TriState.TRUE,
        "C": TriState.FALSE,
        "D": TriState.FALSE,
        "E": TriState.TRUE,
        "F": TriState.FALSE,
        "G": TriState.TRUE,
        "H": TriState.TRUE,
        "I": TriState.FALSE,
    }
    expr = parse_expr(source)

    tree_without_context = visualize_ast(expr)
    assert tree_without_context.strip() == _TREE_WITHOUT_CONTEXT_EXPECTED.strip(), \
        "AST visualization without context does not match expected output"

    tree_with_context = visualize_ast(expr, context)
    assert tree_with_context.strip() == _TREE_WITH_CONTEXT_EXPECTED.strip(), \
        "AST visualization with context does not match expected output"
