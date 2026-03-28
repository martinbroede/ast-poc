import ast
from collections.abc import Mapping
from enum import Enum

from core.exceptions import ASTEvaluationError

Context = Mapping[str, 'TriState']


class TriState(str, Enum):
    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return self.value

    def __and__(self, other: "TriState") -> "TriState":
        """
        TriState AND ('&') operator.
        """
        if self is TriState.FALSE or other is TriState.FALSE:
            return TriState.FALSE
        if self is TriState.TRUE and other is TriState.TRUE:
            return TriState.TRUE
        return TriState.UNKNOWN

    def __or__(self, other: "TriState") -> "TriState":
        """
        TriState OR ('|') operator.
        """
        if self is TriState.TRUE or other is TriState.TRUE:
            return TriState.TRUE
        if self is TriState.FALSE and other is TriState.FALSE:
            return TriState.FALSE
        return TriState.UNKNOWN

    def __invert__(self) -> "TriState":
        """
        TriState NOT ('~') operator.
        """
        if self is TriState.TRUE:
            return TriState.FALSE
        if self is TriState.FALSE:
            return TriState.TRUE
        return TriState.UNKNOWN


def evaluate(expr: ast.expr, context: Context) -> TriState:
    match expr:
        case ast.Name(id=name):
            try:
                return context[name]
            except KeyError:
                raise ASTEvaluationError(f"Variable {name!r} not found in context") from None

        # BitAnd '&' acts as TriState AND
        case ast.BinOp(left=left, op=ast.BitAnd(), right=right):
            return evaluate(left, context) & evaluate(right, context)

        # BitOr '|' acts as TriState OR
        case ast.BinOp(left=left, op=ast.BitOr(), right=right):
            return evaluate(left, context) | evaluate(right, context)

        # Invert '~' acts as TriState NOT
        case ast.UnaryOp(op=ast.Invert(), operand=operand):
            return ~evaluate(operand, context)

        case _:
            raise ASTEvaluationError(f"Unsupported expression: {type(expr)!r}")
